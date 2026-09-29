from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount", "id")
w_part = Window.partitionBy("customer_id", "channel").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

with_windows = (
    df_orders
    .select(
        "*",
        F.first("shipping_fee", ignorenulls=True).over(w).alias("first_known_shipping_fee"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("shipping_fee").over(w_part).alias("shipping_fee_avg_3"),
        F.rank().over(w4).alias("amount_rank"),
    )
)

result = (
    with_windows
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee_avg_3",
        "amount_rank",
        F.when(F.col("channel").rlike("^(mobile|store)$"), "GROUP_A").otherwise("CORE").alias("channel_zone"),
        F.col("sum_order_amount"),
    )
)
