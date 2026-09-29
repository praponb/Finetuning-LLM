from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("started_at"), "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("browser").orderBy(F.col("event_seq").desc(), "id")
w8 = Window.partitionBy("customer_id").orderBy(F.col("event_seq").desc_nulls_last(), "id")
w9 = Window.partitionBy("country", "customer_id").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

scored = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country_2"),
        F.first("browser", ignorenulls=True).over(w5).alias("first_known_browser_2"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([10, 25, 36])), "left_semi")
)

with_windows = (
    not_flagged
    .filter(F.col("event_seq").between(4, 6))
    .withColumn("last4_avg_event_seq", F.avg("event_seq").over(w6))
    .withColumn("last4_min_event_seq", F.min("event_seq").over(w6))
    .withColumn("last4_max_event_seq", F.max("event_seq").over(w6))
    .withColumn("last4_count_event_seq", F.count("event_seq").over(w6))
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.avg("last4_avg_event_seq").over(w9).alias("running_avg_last4_avg_event_seq"),
        F.count("last4_avg_event_seq").over(w9).alias("running_count_last4_avg_event_seq"),
        F.min("last4_avg_event_seq").over(w9).alias("running_min_last4_avg_event_seq"),
    )
)

result = (
    metrics
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "last_known_country",
        "first_known_browser",
        "event_seq",
        "last_known_country_2",
        "first_known_browser_2",
        "last4_min_event_seq",
        F.col("min_account_balance"),
    )
)
