from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", F.lit("customer").alias("source"))
    .union(
        df_orders
        .filter(F.col("status") == "SHIPPED")
        .select("order_date", F.lit("order").alias("source"))
    )
)
