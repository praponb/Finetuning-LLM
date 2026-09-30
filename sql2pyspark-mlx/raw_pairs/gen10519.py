from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["web", "mobile", "store"]))
    .select(
        "customer_id",
        (F.col("discount") - F.col("amount")).alias("discount_minus_amount"),
        F.date_add("order_date", 60).alias("order_date_plus_60d"),
    )
)
