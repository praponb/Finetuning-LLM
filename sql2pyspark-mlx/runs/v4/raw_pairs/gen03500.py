from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("burst_id").desc(), "id")
w4 = Window.partitionBy("order_id", "method").orderBy(F.col("prev_paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("amount", "id")
w7 = Window.orderBy(F.col("amount").asc_nulls_first())

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

deduped = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_prev = deduped.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands_payments = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    islands_payments
    .select(
        "*",
        F.avg("new_group_flag").over(w4).alias("new_group_flag_avg_30d"),
        F.sum("new_group_flag").over(w5).alias("running_new_group_flag"),
        F.lead("new_group_flag", 1).over(w6).alias("next_new_group_flag"),
        F.rank().over(w7).alias("amount_rank"),
    )
)
