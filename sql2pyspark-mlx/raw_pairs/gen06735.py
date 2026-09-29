from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("burst_id", "id")
w5 = Window.partitionBy("customer_country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("event_seq", "id")
w7 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.orderBy(F.col("burst_id").desc())

metrics = df_sessions.withColumn("event_seq", F.row_number().over(w))

joined = (
    metrics
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.signup_date") <= "2023-05-17")
    .select("s.*", F.col("p.country").alias("customer_country"))
)

with_prev = joined.withColumn("prev_started_at", F.lag("started_at").over(w2))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "event_seq",
        "customer_country",
        F.sum("new_group_flag").over(w3).alias("burst_id"),
    )
)

with_windows = (
    sessionized
    .withColumn("burst_id_bucket", F.ntile(3).over(w4))
    .withColumn("event_seq_max_7d", F.max("event_seq").over(w5))
    .withColumn("next_burst_id", F.lead("burst_id", 1).over(w6))
    .withColumn("event_seq_max_90d", F.max("event_seq").over(w7))
    .withColumn("burst_id_dense_rank", F.dense_rank().over(w8))
)

result = (
    with_windows
    .withColumn("activity_month", F.date_trunc("month", "started_at"))
    .withColumn("quarter_start", F.date_trunc("quarter", "started_at"))
    .groupingSets(
        [["activity_month", "next_burst_id", "quarter_start"], ["activity_month", "next_burst_id"]],
        "activity_month",
        "next_burst_id",
        "quarter_start",
    )
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("customer_country") == "JP", 1)).alias("jp_rows"),
        F.max("started_at").alias("last_seen"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("firefox_rows").desc_nulls_first())
)
