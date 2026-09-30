from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "started_at_date"), "country").orderBy(F.col("device_length").asc_nulls_last())
w2 = Window.partitionBy("device").orderBy("device_length")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-07-11").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("device_length", F.length("device"))
)

remaining_keys = (
    filtered
    .select("device", "country")
    .intersect(
        df_sessions
        .filter(F.col("browser").isNotNull())
        .select("device", "country")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.cume_dist().over(w).alias("device_length_cume_dist"),
        F.dense_rank().over(w2).alias("device_length_dense_rank"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

with_dims = (
    kept
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.device",
        "e.browser",
        "e.started_at",
        "e.device_length_dense_rank",
        F.col("b.signup_date"),
        F.col("b.segment"),
        F.col("b.city"),
    )
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([14, 23])), "left_anti")
)

result = (
    not_flagged
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "device_length_dense_rank",
        "signup_date",
        "segment",
        F.col("min_account_balance"),
    )
    .orderBy("customer_id")
)
