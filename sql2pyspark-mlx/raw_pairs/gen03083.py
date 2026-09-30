from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("last4_max_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id", "method").orderBy(F.col("running_sum_last4_count_amount").desc(), "id")
w6 = Window.partitionBy("order_id", "status").orderBy("running_sum_last4_count_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id").orderBy("running_sum_last4_count_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-11-07").cast("date"))
    .select("*", F.concat("method", F.lit(" - "), "status").alias("method_status"))
)

with_windows = (
    valid
    .withColumn("last4_max_amount", F.max("amount").over(w_running))
    .withColumn("last4_sum_amount", F.sum("amount").over(w_running))
    .withColumn("last4_count_amount", F.count("amount").over(w_running))
    .withColumn("running_amount", F.sum("amount").over(w2))
)

with_windows_payments = (
    with_windows
    .filter(F.col("method") == "paypal")
    .withColumn("running_sum_last4_count_amount", F.sum("last4_count_amount").over(w3))
    .withColumn("running_min_last4_count_amount", F.min("last4_count_amount").over(w3))
    .withColumn("running_avg_last4_count_amount", F.avg("last4_count_amount").over(w3))
    .withColumn("running_max_last4_count_amount", F.max("last4_count_amount").over(w3))
    .withColumn("order_id_max_last4_sum_amount", F.max("last4_sum_amount").over(w4))
)

deduped = (
    with_windows_payments
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    deduped
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-08-18"))) & (F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "method_status",
        "last4_max_amount",
        "last4_sum_amount",
        "last4_count_amount",
        "running_sum_last4_count_amount",
        "running_avg_last4_count_amount",
        "running_max_last4_count_amount",
        "rank_in_group",
        (F.col("running_max_last4_count_amount") - F.col("last4_max_amount")).alias("running_max_last4_count_amount_minus_last4_max_amount"),
    )
)

valid_payments = (
    base
    .filter(F.col("last4_count_amount").between(10, 12))
    .select(
        "*",
        (F.col("last4_max_amount") + F.col("running_max_last4_count_amount")).alias("last4_max_amount_plus_running_max_last4_count_amount"),
        F.round((F.col("running_avg_last4_count_amount") / F.col("last4_sum_amount")), 2).alias("running_avg_last4_count_amount_per_last4_sum_amount"),
    )
)

scored = (
    valid_payments
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "last4_max_amount",
        "last4_sum_amount",
        "running_sum_last4_count_amount",
        "running_avg_last4_count_amount",
        "rank_in_group",
        "last4_max_amount_plus_running_max_last4_count_amount",
        F.sum("last4_max_amount").over(w6).alias("running_last4_max_amount"),
        F.sum("running_avg_last4_count_amount_per_last4_sum_amount").over(w_hist).alias("cumulative_running_avg_last4_count_amount_per_last4_sum_amount"),
        F.last("running_sum_last4_count_amount", ignorenulls=True).over(w_ordered).alias("last_known_running_sum_last4_count_amount"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.channel").alias("channel"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.running_last4_max_amount")), 2).alias("total_running_last4_max_amount"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
