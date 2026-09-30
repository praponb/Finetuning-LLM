from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("month", "opened_date").alias("activity_month"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.count("*").alias("row_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft_2"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
    .orderBy(F.col("avg_sqft").desc())
)
