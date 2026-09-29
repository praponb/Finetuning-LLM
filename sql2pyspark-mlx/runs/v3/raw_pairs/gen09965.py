from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 5) & F.col("product_id").isNull() & (F.col("quantity") >= 6))
    .select("unit_price")
    .orderBy("unit_price")
)
