from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("customer_id", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("started_at", "id")
w6 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("browser").orderBy(F.col("avg_is_new_session").desc())

with_windows = (
    df_sessions
    .filter(F.to_date("started_at") >= "2025-03-01")
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w4))
)

gaps = (
    with_windows
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w5) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w6))

with_dims = (
    islands
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.browser",
        "a.country",
        "a.row_seq",
        "a.event_seq",
        "a.first_known_country",
        "a.is_new_session",
        "a.session_no",
        F.col("p.country").alias("customer_country"),
        F.col("p.credit_limit"),
    )
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < "2023-11-21"), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        "browser",
        F.when(F.col("credit_limit") >= 17700, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
        "customer_country",
    )
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.sum("is_new_session").alias("total_is_new_session"),
    )
    .filter(F.col("avg_is_new_session") >= 0)
    .withColumn("avg_is_new_session_dense_rank", F.dense_rank().over(w7))
    .orderBy(F.col("total_is_new_session").desc())
)
