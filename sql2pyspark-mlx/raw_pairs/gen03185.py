from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .select(
        "id",
        "amount",
        "status",
        "session_no",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "GROUP_A").when(F.col("status").rlike("^(FAILED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(10)
)
