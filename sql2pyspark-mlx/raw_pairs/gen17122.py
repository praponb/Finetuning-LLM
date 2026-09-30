from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.hour("paid_at").between(11, 19))
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.round((F.col("amount") * 1.15), 2).alias("amount_scaled"),
    )
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1000)
    .orderBy(F.col("year_start").desc_nulls_first())
)
