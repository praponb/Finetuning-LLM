from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("product_id").isin([10, 12, 19, 27]))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low"))
)

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    product_id_summary
    .select("product_id", "avg_quantity")
    .orderBy(F.col("product_id").desc(), F.col("avg_quantity").desc())
)
