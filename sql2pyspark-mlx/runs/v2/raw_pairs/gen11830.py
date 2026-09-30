from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2023-12-01")
    .select("id", "status")
    .subtract(df_orders.filter(F.col("discount") < 0.35).select("id", "status"))
    .orderBy("status")
)
