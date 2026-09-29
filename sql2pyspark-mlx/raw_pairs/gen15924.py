from pyspark.sql import functions as F

result = (
    df_inventory
    .cube(
        "store_id",
        F.when(F.col("quantity") >= 180, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .filter(F.col("distinct_ids_2") < 1000)
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
