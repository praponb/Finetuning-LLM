from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("started_at"), "browser")
w4 = Window.partitionBy(F.date_trunc("MONTH", "started_at"), "device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("new_group_flag", "id").rowsBetween(-2, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-07-23 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.substring("device", 1, 2).alias("device_prefix"),
        F.coalesce("country", F.lit("DIRECT")).alias("resolved_country"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page") != "/product"), "left_semi")
)

with_gaps = (
    not_flagged
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

scored = (
    sessions_sessions
    .filter(F.col("customer_id").isin([10, 19, 21, 28]))
    .select(
        "id",
        "customer_id",
        "island_id",
        F.count("new_group_flag").over(w3).alias("started_at_year_count_new_group_flag"),
        F.sum("island_id").over(w4).alias("island_id_sum_30d"),
        F.sum("island_id").over(w5).alias("running_island_id"),
        F.sum("new_group_flag").over(w6).alias("cumulative_new_group_flag"),
        F.avg("island_id").over(w7).alias("island_id_avg_2"),
    )
)

result = (
    scored
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "island_id",
        "started_at_year_count_new_group_flag",
        "cumulative_new_group_flag",
        "island_id_avg_2",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("island_id_avg_2").asc_nulls_first(), "cumulative_new_group_flag")
)
