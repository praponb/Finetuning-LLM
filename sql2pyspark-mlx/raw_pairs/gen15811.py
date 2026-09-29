from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-12-31")
    .withColumn("quantity_band", F.when(F.col("quantity") >= 228, "High").when(F.col("quantity") >= 71, "Medium").otherwise("Low"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("quantity") >= 251, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.avg("quantity").alias("avg_quantity_2"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .orderBy(F.col("avg_quantity").desc_nulls_first())
)
