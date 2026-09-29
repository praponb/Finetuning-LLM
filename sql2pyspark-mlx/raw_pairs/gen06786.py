from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_no") >= 7, "HIGH").otherwise("LOW").alias("session_no_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
    )
    .filter(F.col("avg_is_new_burst") > 0)
    .orderBy(F.col("avg_is_new_burst").asc_nulls_first())
)
