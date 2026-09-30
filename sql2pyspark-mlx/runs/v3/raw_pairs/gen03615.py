from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([28, 36, 39]))
    .select("id", "unit_price", "product_id", "quantity")
)
