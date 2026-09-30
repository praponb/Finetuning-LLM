from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessions_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.when(F.col("status").rlike("^(FAILED|REFUNDED)$"), "GROUP_A").when(F.col("status").rlike("^(SUCCESS)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("paid_at").desc_nulls_first())
)
