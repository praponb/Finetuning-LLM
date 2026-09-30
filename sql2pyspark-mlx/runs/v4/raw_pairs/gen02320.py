from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("paid_at").desc_nulls_first())
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc_nulls_last(), "id")
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("rn", "id")

deduped = (
    df_payments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

deduped_payments = (
    deduped
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
)

joined = (
    deduped_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.channel"))
)

with_gaps = (
    joined
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w4))

deduped_payments_2 = (
    islands
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

grouped = (
    deduped_payments_2
    .groupBy(
        F.when(F.col("session_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.min("session_start_flag").alias("min_session_start_flag"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
    )
)

result = (
    grouped
    .select("session_id_tier", "num_status")
    .orderBy(F.col("session_id_tier").desc())
)
