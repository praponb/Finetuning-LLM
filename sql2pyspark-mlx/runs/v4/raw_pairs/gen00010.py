from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", F.date_trunc("YEAR", "last_restock").alias("year_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("event_count_3"),
    )
    .orderBy(F.col("event_count_3").desc_nulls_first())
)
