from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .groupingSets([["status", "year_start"], ["status"]], "status", "year_start")
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount"))).alias("refunded_total_amount"),
        F.max(F.when(F.col("status") == "REFUNDED", F.col("is_new_session"))).alias("refunded_max_is_new_session"),
        F.grouping("status").alias("grouping_level"),
    )
    .orderBy(F.col("refunded_total_amount").desc_nulls_last())
)
