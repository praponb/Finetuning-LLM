from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.filter(F.col("store_id").isNotNull()).alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left_anti")
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("customers.age"),
    )
)
