from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("started_at", "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("session_start_flag").desc_nulls_first())
w6 = Window.partitionBy(F.date_trunc("week", "started_at"), "country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w8 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

with_gaps = (
    sessionized
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "session_start_flag",
        "burst_id",
        F.sum("is_new_session").over(w4).alias("burst_id_2"),
    )
)

with_windows = (
    islands
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "session_start_flag",
        "burst_id",
        "burst_id_2",
        F.cume_dist().over(w5).alias("session_start_flag_cume_dist"),
        F.sum("burst_id").over(w6).alias("burst_id_sum_30d"),
    )
)

flagged_2 = (
    with_windows
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w7).isNull() | (F.col("started_at") > F.lag("started_at").over(w7) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_sessions = (
    flagged_2
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "burst_id",
        "burst_id_2",
        "session_start_flag_cume_dist",
        F.sum("is_new_burst").over(w8).alias("island_id"),
    )
)

result = (
    sessionized_sessions
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "left")
    .rollup(
        F.col("dim.country").alias("customer_country"),
        F.col("a.country").alias("session_country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.island_id")), 2).alias("total_island_id"),
        F.min(F.col("a.burst_id_2")).alias("min_burst_id_2"),
    )
)
