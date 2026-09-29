from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.partitionBy("order_id")

ranked = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

gaps = (
    ranked
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w3))

scored = (
    sessionized
    .withColumn("is_new_session_count_7d", F.count("is_new_session").over(w_part))
    .withColumn("order_id_max_amount", F.max("amount").over(w_recent))
)

stats = (
    scored
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
        "island_id",
    )
    .agg(
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.sum("is_new_session").alias("total_is_new_session"),
    )
)

result = (
    stats
    .select("is_new_session_tier", "island_id", "avg_is_new_session", "method_list")
    .orderBy(F.col("avg_is_new_session").asc_nulls_first())
)
