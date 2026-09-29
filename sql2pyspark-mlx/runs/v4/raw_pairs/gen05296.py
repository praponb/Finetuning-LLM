from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .select("id", "amount", "prev_paid_at", "new_group_flag", "island_id")
    .orderBy(F.col("new_group_flag").desc_nulls_last(), F.col("island_id").desc())
)
