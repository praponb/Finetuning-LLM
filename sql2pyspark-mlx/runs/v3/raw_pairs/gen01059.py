from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") != "partner") & ((F.col("amount") < 660) | (F.col("shipping_fee") < 25)))
    .select("discount", F.last_day("order_date").alias("order_date_month_end"))
    .orderBy(F.col("discount").desc())
)
