from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select("signup_date")
    .unionByName(
        df_orders
        .filter(F.col("order_date").isNotNull())
        .select(F.col("order_date").alias("signup_date"))
    )
    .distinct()
)
