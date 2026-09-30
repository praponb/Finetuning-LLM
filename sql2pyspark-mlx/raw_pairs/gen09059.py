from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 5) & F.col("product_id").isNotNull())
    .withColumn("quantity_num", F.col("quantity").cast("double"))
    .orderBy(F.col("order_id").desc(), F.col("product_id").desc())
)
