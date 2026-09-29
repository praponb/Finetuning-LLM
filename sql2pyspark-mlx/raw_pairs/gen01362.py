from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 21)
    .select(
        "store_id",
        "discount",
        "status",
        F.last_day("order_date").alias("order_date_month_end"),
    )
)
