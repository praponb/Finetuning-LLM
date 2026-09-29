from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w_hist = Window.partitionBy(F.year("started_at"), "browser").orderBy("started_at", "id")

with_windows = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w_hist))
)

attributed = (
    with_windows
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    attributed
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("a.*", F.col("c.signup_date"))
)

kept_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_sessions
        .filter(F.col("customer_id") == 7)
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .rollup(F.col("c.city").alias("city"), F.col("a.browser").alias("browser"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.row_seq")).alias("max_row_seq"),
        F.grouping_id().alias("grouping_level"),
    )
)
