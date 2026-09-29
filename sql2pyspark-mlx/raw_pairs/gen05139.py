from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") < "2023-07-01")
    .select(
        "id",
        "status",
        "order_date",
        "customer_id",
        F.initcap(F.trim("status")).alias("status_title"),
        F.date_trunc("month", "order_date").alias("order_date_period"),
    )
)
