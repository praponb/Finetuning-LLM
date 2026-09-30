from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

remaining_keys = (
    df_sessions
    .select("id", "country")
    .intersectAll(
        df_sessions
        .filter(F.col("country").isin(["DE", "JP", "UK"]))
        .select("id", "country")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("started_at") > F.lit("2023-03-02 20:00:00").cast("timestamp"))
    .select(
        "*",
        F.substring("country", 1, 4).alias("country_prefix"),
        F.month("started_at").alias("started_at_month"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.credit_limit") < 10700)
    .select("src.*", F.col("d.credit_limit"))
)

eligible = (
    joined
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_semi")
)

with_dims = (
    eligible
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.*",
        F.col("c.credit_limit").alias("customer_credit_limit"),
        F.col("c.segment"),
    )
)

filtered = (
    with_dims
    .filter((F.col("started_at") >= F.lit("2023-06-03").cast("date")) & F.col("device").isin(["tablet", "mobile"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "started_at_month",
        "credit_limit",
        "segment",
        F.hour("started_at").alias("started_at_hour"),
        F.col("customer_credit_limit").cast("int").alias("customer_credit_limit_int"),
    )
)

eligible_sessions = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page") == "/home"), "left_anti")
)

result = (
    eligible_sessions
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "credit_limit",
        "segment",
        "started_at_hour",
        "customer_credit_limit_int",
        F.when(F.col("country").rlike("^(SG|UK)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "CORE").otherwise("GROUP_B").alias("country_zone"),
        F.col("min_order_discount"),
    )
    .orderBy("browser")
)
