from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("credit_limit_filled", "id")
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("cumulative_credit_limit_filled").desc(), "id")
w6 = Window.orderBy(F.col("seq_no").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-04-17"))) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "city",
        "is_active",
        "age",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.coalesce("segment", F.lit("DIRECT")).alias("resolved_segment"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "name",
        F.sum("credit_limit_filled").over(w).alias("cumulative_credit_limit_filled"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("credit_limit_filled").over(w3).alias("credit_limit_filled_avg_3"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.monthly_fee") >= 70), "left_semi")
)

with_windows = (
    kept
    .select(
        "*",
        F.first("cumulative_credit_limit_filled", ignorenulls=True).over(w4).alias("first_known_cumulative_credit_limit_filled"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(4).over(w6).alias("seq_no_bucket"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.row_seq", "s.seq_no_bucket", F.col("c.device"))
)

kept_keys = (
    with_latest
    .select("id")
    .intersectAll(
        df_customers
        .filter(~F.col("is_active"))
        .select("id")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "seq_no_bucket",
        "device",
        F.when(F.col("device").rlike("^(mobile|desktop)$"), "CORE").otherwise("OTHER_ZONE").alias("device_zone"),
    )
)
