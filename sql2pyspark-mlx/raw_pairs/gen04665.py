from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("method").orderBy("session_start_flag", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("method").orderBy("burst_id_avg_3_prior", "id")
w6 = Window.partitionBy("method").orderBy(F.col("burst_id_avg_3_prior").asc_nulls_first())

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

valid = (
    sessionized
    .filter(F.col("burst_id") >= 4)
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "session_start_flag",
        "burst_id",
        F.least("amount", "burst_id").alias("least_amount_burst_id"),
    )
)

with_windows = (
    valid
    .filter(F.col("order_id").isin([3, 17, 18, 29]))
    .select(
        "id",
        "method",
        "amount",
        "session_start_flag",
        "burst_id",
        "least_amount_burst_id",
        F.sum("burst_id").over(w3).alias("cumulative_burst_id"),
        F.avg("burst_id").over(w_hist).alias("burst_id_avg_3_prior"),
    )
)

scored = (
    with_windows
    .withColumn("prev_least_amount_burst_id", F.lag("least_amount_burst_id", 1).over(w5))
    .withColumn("burst_id_avg_3_prior_dense_rank", F.dense_rank().over(w6))
)

base = (
    scored
    .filter(F.col("method").isNotNull() & (F.col("least_amount_burst_id") < 44))
    .select("*", F.floor("burst_id").alias("burst_id_whole"))
)

result = (
    base
    .select(
        "id",
        "amount",
        "session_start_flag",
        "burst_id_avg_3_prior",
        "prev_least_amount_burst_id",
        "burst_id_avg_3_prior_dense_rank",
        "burst_id_whole",
    )
    .orderBy(F.col("amount").desc_nulls_last(), "prev_least_amount_burst_id")
)
