from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

result = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)
