from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2025-02-01", "2025-03-01"))
    .select("status")
    .unionByName(df_orders.filter(F.col("amount") > 770).select("status"))
    .orderBy("status")
)
