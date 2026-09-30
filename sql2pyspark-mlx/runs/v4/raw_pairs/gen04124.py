from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL"))
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["activity_month", "is_new_session_tier", "burst_id_tier"], ["activity_month", "is_new_session_tier"], []],
        "activity_month",
        "is_new_session_tier",
        "burst_id_tier",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("method") == "bank_transfer").alias("bank_transfer_events"),
    )
)
