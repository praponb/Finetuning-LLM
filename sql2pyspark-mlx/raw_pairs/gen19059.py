from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "is_new_burst",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "CORE").when(F.col("status").rlike("^(FAILED)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("order_id").desc_nulls_first())
)
