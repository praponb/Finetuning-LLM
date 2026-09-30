from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.32)
    .select(
        "id",
        "status",
        F.quarter("order_date").alias("order_date_quarter"),
        F.length("status").alias("status_length"),
    )
)
