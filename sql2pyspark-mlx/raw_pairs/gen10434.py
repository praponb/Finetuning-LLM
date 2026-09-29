from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")
w_part = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy(F.year("paid_at"), "status")
w4 = Window.partitionBy("order_id")
w5 = Window.orderBy("last3_max_amount", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("order_id").orderBy(F.col("last3_min_amount").desc_nulls_last())
w7 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.month("paid_at"), "status").orderBy(F.col("amount").desc_nulls_last())

ranked = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    ranked
    .filter(F.col("order_id") == 30)
    .select(
        "*",
        F.avg("amount").over(w_part).alias("last3_avg_amount"),
        F.min("amount").over(w_part).alias("last3_min_amount"),
        F.max("amount").over(w_part).alias("last3_max_amount"),
    )
)

with_windows_payments = (
    with_windows
    .withColumn("paid_at_year_avg_last3_max_amount", F.avg("last3_max_amount").over(w3))
    .withColumn("last3_max_amount_share", F.round((F.col("last3_max_amount") / F.sum("last3_max_amount").over(w4)), 4))
    .withColumn("last3_avg_amount_avg_2_prior", F.avg("last3_avg_amount").over(w5))
    .withColumn("last3_min_amount_percent_rank", F.percent_rank().over(w6))
)

enriched = (
    with_windows_payments
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "last3_avg_amount",
        "last3_max_amount",
        "last3_max_amount_share",
        "last3_avg_amount_avg_2_prior",
        F.sum("last3_max_amount").over(w7).alias("cumulative_last3_max_amount"),
        F.rank().over(w8).alias("amount_rank"),
    )
)

result = (
    enriched
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.channel").alias("channel"),
        F.col("d.status").alias("order_status"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.cumulative_last3_max_amount")), 2).alias("total_cumulative_last3_max_amount"),
        F.round(F.sum(F.col("a.amount_rank")), 2).alias("total_amount_rank"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
