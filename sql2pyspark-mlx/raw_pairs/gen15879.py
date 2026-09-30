from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region", "city")
w4 = Window.partitionBy("region", "city").orderBy("opened_date", "id")
w5 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("West"))))
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("DIRECT")))
)

kept = (
    valid
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.revenue").between(1100, 1900)), "left_anti")
)

latest = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.max("sqft").over(w2).alias("region_max_sqft"),
        F.avg("sqft").over(w3).alias("region_avg_sqft"),
        F.lead("sqft", 1).over(w4).alias("next_sqft"),
        F.percent_rank().over(w5).alias("sqft_percent_rank"),
    )
)

result = (
    enriched
    .groupBy("region")
    .pivot("city", ["New York", "Chiang Mai", "Berlin"])
    .agg(
        F.max("sqft_percent_rank").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "region",
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_item_count").alias("new_york_item_count"),
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_item_count").alias("chiang_mai_item_count"),
        F.col("Berlin_amount").alias("berlin_amount"),
        F.col("Berlin_item_count").alias("berlin_item_count"),
    )
)
