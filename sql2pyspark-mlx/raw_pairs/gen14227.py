from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first())
w_recent = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sqft_adjusted", "id")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("region").orderBy(F.col("last_known_sqft_adjusted").desc(), "id")
w6 = Window.partitionBy("region")
w7 = Window.partitionBy("region", "city")
w8 = Window.partitionBy("city").orderBy(F.col("sqft_share").desc(), "id")

base = (
    df_stores
    .filter(F.month("opened_date").isin([3, 7, 8]))
    .withColumn("sqft_adjusted", F.round((F.col("sqft") * 1.07), 2))
    .withColumn("region_clean", F.coalesce("region", F.lit("DIRECT")))
)

scored = (
    base
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "sqft_adjusted",
        "region_clean",
        F.percent_rank().over(w_running).alias("sqft_percent_rank"),
        F.last("sqft_adjusted", ignorenulls=True).over(w_recent).alias("last_known_sqft_adjusted"),
    )
)

enriched = (
    scored
    .filter(F.col("sqft").between(8269, 8583))
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.min("last_known_sqft_adjusted").over(w4).alias("city_min_last_known_sqft_adjusted"),
        F.ntile(4).over(w5).alias("last_known_sqft_adjusted_bucket"),
        F.avg("sqft").over(w6).alias("region_avg_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w7)), 4).alias("sqft_share"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "store_name",
        "sqft",
        "sqft_adjusted",
        "region_clean",
        "sqft_percent_rank",
        "event_seq",
        "last_known_sqft_adjusted_bucket",
        "sqft_share",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Mall Central)$"), "EXTENDED").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "OTHER_ZONE").otherwise("CORE").alias("store_name_zone"),
    )
    .orderBy(F.col("last_known_sqft_adjusted_bucket").desc(), F.col("id").desc())
)
