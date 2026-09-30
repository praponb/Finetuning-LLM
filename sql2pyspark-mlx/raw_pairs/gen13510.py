from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "island_id",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("island_id").desc())
)
