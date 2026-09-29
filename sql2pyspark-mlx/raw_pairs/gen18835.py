from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country", "account_status").orderBy("age")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-02-17")))
    .select("*", F.trim("segment").alias("clean_segment"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.year(F.col("ch.end_date")) >= 2024), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("account_status"))
)

latest = (
    with_latest
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "clean_segment",
    )
)

with_last = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.is_active",
        "s.clean_segment",
        F.col("c.device").alias("session_device"),
        F.col("c.started_at").alias("session_started_at"),
    )
)

remaining_keys = (
    with_last
    .select("id", "city")
    .subtract(
        df_customers
        .filter(~F.col("segment").isin(["Consumer", "SMB"]))
        .select("id", "city")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

cleaned_customers = (
    reconciled
    .filter((~F.col("city").eqNullSafe(F.lit("Tokyo"))) & F.col("session_device").isin(["desktop", "tablet"]))
    .select(
        "id",
        "country",
        "is_active",
        "session_started_at",
        F.to_date("session_started_at").alias("session_started_at_date"),
    )
)

result = (
    cleaned_customers
    .groupBy("country")
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
    )
    .orderBy(F.col("cnt").desc())
)
