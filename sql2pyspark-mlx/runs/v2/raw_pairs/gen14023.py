from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([12, 14]))
    .select("id", "order_id")
)
