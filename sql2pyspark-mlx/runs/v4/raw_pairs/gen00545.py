from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device", "customer_id").orderBy("is_new_burst", "id")
w6 = Window.partitionBy("device").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy(F.col("is_new_burst").desc(), "id")
w8 = Window.partitionBy("device")
w9 = Window.partitionBy("device").orderBy("session_start_flag")
w10 = Window.partitionBy("browser").orderBy(F.col("device_max_is_new_burst").desc_nulls_last())
w11 = Window.partitionBy("browser").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("segment").orderBy(F.col("session_start_flag").desc(), "id")

valid = (
    df_sessions
    .filter(F.col("customer_id").isin([31, 39]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.to_date("started_at").alias("started_at_date"),
    )
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

not_flagged = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_semi")
)

flagged = (
    not_flagged
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w4))

enriched = (
    islands
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("first_known_is_new_burst", F.first("is_new_burst", ignorenulls=True).over(w6))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("device_max_is_new_burst", F.max("is_new_burst").over(w8))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.segment"))
)

latest = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w9))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .withColumn("device_max_is_new_burst_cume_dist", F.cume_dist().over(w10))
    .withColumn("cumulative_burst_id", F.sum("burst_id").over(w11))
    .withColumn("seq_no_3", F.row_number().over(w12))
)
