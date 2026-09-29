from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.41, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("status")).alias("status_list_2"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .orderBy(F.col("discount_tier").desc_nulls_last())
)
