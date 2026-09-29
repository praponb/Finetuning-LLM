from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(190, 240))
    .select("product_id", "unit_price", "quantity")
)
