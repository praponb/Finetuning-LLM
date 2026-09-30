from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("status")
    .agg(
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("session_id_per_card"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.countDistinct("status").alias("distinct_status"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
