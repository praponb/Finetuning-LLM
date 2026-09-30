from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("quantity_rounded", F.round("quantity", 1))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 1.07), 2))
    .orderBy(F.col("quantity_scaled").desc())
)
