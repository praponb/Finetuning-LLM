from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-03-23")))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.floor("quantity").alias("quantity_whole"),
        F.col("quantity").cast("double").alias("quantity_num"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("quantity") >= 228, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "store_id",
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("quantity_whole", 0.9).alias("quantity_whole_p90"),
        F.countDistinct("store_id").alias("distinct_store_ids_2"),
        F.countDistinct("store_id").alias("distinct_store_ids_3"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("distinct_store_ids_2") > 1)
    .orderBy(F.col("distinct_store_ids").desc_nulls_last())
)
