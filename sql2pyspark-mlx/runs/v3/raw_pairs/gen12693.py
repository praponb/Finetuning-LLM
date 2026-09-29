from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("order_id", "quantity")
    .orderBy("quantity", F.col("order_id").desc())
)
