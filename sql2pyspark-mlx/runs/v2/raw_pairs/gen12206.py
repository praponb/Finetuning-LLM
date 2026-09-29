from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 4, 6]) & F.col("store_id").isin([5, 6])))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 206, "High").when(F.col("quantity") >= 77, "Medium").otherwise("Low"))
)

product_id_summary = (
    base
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    product_id_summary
    .filter(F.col("quantity_count") > 6)
    .orderBy(F.col("product_id").desc())
)
