from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("amount") > 1480)

payments_stats = (
    valid_payments
    .groupBy("order_id", F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = payments_stats.orderBy("min_paid_at")
