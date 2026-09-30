from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 31) & (F.col("order_date") <= "2025-04-21"))
    .select("id", "amount", "status")
    .orderBy(F.col("id").desc())
)
