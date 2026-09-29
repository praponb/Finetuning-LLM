from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 8, 12]) & ((F.col("product_id") == 7) | (F.col("quantity") > 3)))
    .select("id", "quantity", "order_id", "unit_price")
    .orderBy(F.col("unit_price").desc(), F.col("quantity").desc())
)
