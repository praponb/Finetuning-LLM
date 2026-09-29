from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") < "2023-08-23") & F.col("order_date").between("2023-05-12", "2025-06-12"))
    .select("id", "store_id", "amount")
    .orderBy("id")
    .limit(10)
)
