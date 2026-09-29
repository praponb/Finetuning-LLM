from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([25, 34]))
    .select("quantity")
)
