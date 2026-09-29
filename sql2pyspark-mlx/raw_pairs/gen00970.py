from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-04-17"))) & F.col("segment").isNotNull())
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

with_last = (
    filtered
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device"), F.col("c.browser"))
)

kept_keys = (
    with_last
    .select("id")
    .intersect(
        df_customers
        .filter(F.col("is_active"))
        .select("id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("country", "id")
    .exceptAll(
        df_customers
        .filter(F.col("city").isin(["Tokyo", "Bangkok"]))
        .select("country", "id")
    )
)

verified_customers = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified_customers
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("credit_limit") >= 17200, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.sum(F.when(F.col("browser") == "Edge", F.col("credit_limit"))).alias("edge_total_credit_limit"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.percentile_approx("is_active_flag", [0.25, 0.5, 0.75]).alias("is_active_flag_quartiles"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
    )
    .orderBy("week_start")
)
