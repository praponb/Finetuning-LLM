from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 760)
    .select("order_date")
    .intersect(df_customers.select("signup_date"))
)
