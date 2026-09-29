from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "status",
        F.last_day("order_date").alias("order_date_month_end"),
    )
    .orderBy(F.col("order_date_month_end").desc(), F.col("store_id").desc(), "id")
    .limit(5)
)
