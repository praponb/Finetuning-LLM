from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("opened_date", "id")
w4 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("cumulative_sqft").desc(), "id")
w6 = Window.partitionBy("city").orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("YEAR", "opened_date_month_end"), "city").orderBy(F.col("sqft_filled").desc(), "id")

with_windows = (
    df_stores
    .withColumn("last_known_sqft", F.last("sqft", ignorenulls=True).over(w))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w2))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

filtered = (
    deduped
    .filter((F.col("opened_date") > F.lit("2023-08-17").cast("date")) & (~F.col("region").eqNullSafe(F.lit("West"))))
    .select(
        "id",
        "store_name",
        "city",
        "last_known_sqft",
        "cumulative_sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "store_name",
        "city",
        "last_known_sqft",
        "cumulative_sqft",
        "region_filled",
        F.ntile(4).over(w5).alias("cumulative_sqft_bucket"),
        F.sum("last_known_sqft").over(w6).alias("running_last_known_sqft"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_name",
        "last_known_sqft",
        "cumulative_sqft",
        "region_filled",
        "row_seq",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Mall Central)$"), "GROUP_A").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "GROUP_B").otherwise("CORE").alias("store_name_zone"),
    )
    .orderBy(F.col("cumulative_sqft").desc_nulls_last())
)
