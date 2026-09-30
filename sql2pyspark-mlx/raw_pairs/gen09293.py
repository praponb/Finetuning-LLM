from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "method",
        "amount",
        "status",
        "prev_paid_at",
        "session_start_flag",
        "island_id",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "EXTENDED").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("amount").desc_nulls_first())
)
