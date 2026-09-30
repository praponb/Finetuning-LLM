from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .union(df_orders.filter(F.col("discount") > 0.33).select("order_date"))
    .orderBy("signup_date")
)
