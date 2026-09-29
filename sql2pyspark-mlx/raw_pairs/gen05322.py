from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

result = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)
