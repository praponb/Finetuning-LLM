from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("island_id_tier").orderBy(F.col("bank_transfer_events").desc())

with_gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("island_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("method") == "card").alias("card_events"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count_if(F.col("method") == "bank_transfer").alias("bank_transfer_events"),
    )
    .withColumn("bank_transfer_events_dense_rank", F.dense_rank().over(w3))
    .orderBy(F.col("amount_stddev").desc())
)
