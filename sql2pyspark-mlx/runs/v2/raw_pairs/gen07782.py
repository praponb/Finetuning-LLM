from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("order_id") == 27) & F.col("unit_price").between(150, 340)) | (F.col("quantity") != 5))
    .select("quantity", "product_id", "order_id")
    .orderBy(F.col("quantity").desc(), F.col("product_id").desc())
)
