from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .intersect(df_customers.filter(F.col("country") != "UK").select("signup_date"))
)
