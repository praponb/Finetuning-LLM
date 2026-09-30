from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([24, 25]))
    .select("quantity", "unit_price", "order_id")
    .orderBy(F.col("unit_price").desc())
)
