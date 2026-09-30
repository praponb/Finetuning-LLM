from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "session_id",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(REFUNDED)$"), "GROUP_B").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("session_id").desc_nulls_last())
)
