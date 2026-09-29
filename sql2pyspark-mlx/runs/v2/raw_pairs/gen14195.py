from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") >= 0.18) & (F.col("discount") >= 0.39) & (F.col("order_date") <= "2025-02-01"))
    .select("id", "discount", "amount", "customer_id")
    .orderBy(F.col("discount").desc(), "id")
    .limit(20)
)
