from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") == 2024)
    .select("customer_id", "order_date")
    .orderBy(F.col("order_date").desc())
)
