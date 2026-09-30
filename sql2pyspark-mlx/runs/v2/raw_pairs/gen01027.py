from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.26) | (F.col("discount") <= 0.37))
    .select(
        "id",
        "channel",
        "discount",
        F.last_day("order_date").alias("order_date_month_end"),
    )
)
