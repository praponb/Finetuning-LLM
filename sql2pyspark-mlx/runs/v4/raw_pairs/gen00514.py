from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

enriched = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

matched_keys = (
    enriched
    .select("customer_id", "browser")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["TH", "DE", "SG"]))
        .select("customer_id", "browser")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("customer_id", "device")
    .intersectAll(
        df_sessions
        .filter(F.col("customer_id") == 11)
        .select("customer_id", "device")
    )
)

verified_sessions = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    verified_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "event_seq",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("browser").asc_nulls_last())
)
