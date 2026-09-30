from pyspark.sql import functions as F

result = (
    df_order_items
    .select("product_id", "quantity")
    .orderBy(F.col("product_id").desc(), "quantity")
)
