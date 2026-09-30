from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("unit_price").between(120, 140) & F.col("unit_price").between(340, 380) & (F.col("unit_price") <= 390)))
    .select("order_id", "unit_price")
)
