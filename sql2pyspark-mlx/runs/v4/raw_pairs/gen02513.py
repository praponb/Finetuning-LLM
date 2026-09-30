from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("region").orderBy(F.col("sqft_avg_2").desc_nulls_first(), "id")
w6 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("region").orderBy("sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("sqft") >= 9740)
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

filtered = (
    cleaned
    .filter(F.col("opened_date") > F.lit("2023-05-09").cast("date"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "store_name_title",
        F.concat("region", F.lit(" - "), "store_name").alias("region_store_name"),
        F.concat("store_name", F.lit(" - "), "region").alias("store_name_region"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
        F.max("sqft").over(w2).alias("region_max_sqft"),
        F.ntile(5).over(w3).alias("sqft_bucket"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.avg("sqft").over(w4).alias("sqft_avg_2"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

eligible = (
    latest
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_semi")
)

result = (
    eligible
    .withColumn("running_sqft", F.sum("sqft").over(w6))
    .withColumn("last_known_sqft_avg_2", F.last("sqft_avg_2", ignorenulls=True).over(w7))
)
