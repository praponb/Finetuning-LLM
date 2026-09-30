from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount").asc_nulls_last())
w2 = Window.orderBy("discount", "id")

base = (
    df_orders
    .filter(F.col("status").isin(["CANCELLED", "PENDING"]) & (F.col("order_date") >= F.lit("2023-06-18").cast("date")))
    .withColumn("order_date_minus_65d", F.date_sub("order_date", 65))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        F.cume_dist().over(w).alias("discount_cume_dist"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
