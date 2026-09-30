from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "is_new_burst",
        "session_id",
        F.when(F.col("method").rlike("^(paypal|bank_transfer)$"), "GROUP_B").otherwise("PRIORITY").alias("method_zone"),
    )
)
