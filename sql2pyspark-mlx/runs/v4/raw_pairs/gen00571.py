from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select("id", "order_id", F.floor("quantity").alias("quantity_whole"))
    .orderBy(F.col("id").desc())
)
