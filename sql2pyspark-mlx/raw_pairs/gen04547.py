from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser")
w2 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy("device", "browser").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("browser", "country")
w5 = Window.partitionBy("device").orderBy(F.col("credit_limit_percent_rank").desc_nulls_last())
w6 = Window.orderBy(F.col("browser_count_credit_limit").desc())
w7 = Window.partitionBy("browser").orderBy(F.col("browser_count_credit_limit").desc_nulls_last())

filtered = (
    df_sessions
    .filter(~F.col("country").eqNullSafe(F.lit("SG")))
    .withColumn("clean_country", F.trim("country"))
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("DIRECT")))
)

matched_keys = (
    filtered
    .select("customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("device").isin(["mobile", "tablet", "desktop"]))
        .select("customer_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.length(F.col("p.email")) >= 8)
    .select(
        "a.customer_id",
        "a.device",
        "a.browser",
        "a.country",
        "a.clean_country",
        "a.resolved_browser",
        F.col("p.signup_date"),
        F.col("p.credit_limit"),
        F.col("p.segment"),
    )
)

scored = (
    joined
    .select(
        "*",
        F.count("credit_limit").over(w).alias("browser_count_credit_limit"),
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
    )
)

scored_sessions = (
    scored
    .select(
        "device",
        "browser",
        "country",
        "signup_date",
        "credit_limit",
        "browser_count_credit_limit",
        "credit_limit_dense_rank",
        F.percent_rank().over(w3).alias("credit_limit_percent_rank"),
        F.avg("browser_count_credit_limit").over(w).alias("browser_avg_browser_count_credit_limit"),
        F.stddev_samp("browser_count_credit_limit").over(w4).alias("browser_stddev_samp_browser_count_credit_limit"),
    )
)

scored_sessions_2 = (
    scored_sessions
    .select(
        "browser",
        "browser_count_credit_limit",
        "credit_limit_dense_rank",
        "browser_avg_browser_count_credit_limit",
        F.dense_rank().over(w5).alias("credit_limit_percent_rank_dense_rank"),
        F.percent_rank().over(w6).alias("browser_count_credit_limit_percent_rank"),
        F.dense_rank().over(w7).alias("browser_count_credit_limit_dense_rank"),
    )
)

filtered_sessions = (
    scored_sessions_2
    .filter(F.col("browser_count_credit_limit") == 8)
    .select(
        "browser",
        "credit_limit_dense_rank",
        "browser_avg_browser_count_credit_limit",
        "credit_limit_percent_rank_dense_rank",
        "browser_count_credit_limit_percent_rank",
        "browser_count_credit_limit_dense_rank",
        F.lower("browser").alias("browser_lower"),
        F.round((F.col("browser_avg_browser_count_credit_limit") / F.col("browser_count_credit_limit")), 2).alias("browser_avg_browser_count_credit_limit_per_browser_count_credit_limit"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

totals = (
    filtered_sessions
    .withColumn("browser_avg_browser_count_credit_limit_tier", F.when(F.col("browser_avg_browser_count_credit_limit") >= 17, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["browser_avg_browser_count_credit_limit_tier", "browser"], ["browser_avg_browser_count_credit_limit_tier"], []],
        "browser_avg_browser_count_credit_limit_tier",
        "browser",
    )
    .agg(
        F.sum("credit_limit_percent_rank_dense_rank").alias("total_credit_limit_percent_rank_dense_rank"),
        F.round(F.sum("browser_count_credit_limit_percent_rank") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("browser_count_credit_limit_percent_rank_per_firefox"),
        F.sum("credit_limit_percent_rank_dense_rank").alias("total_credit_limit_percent_rank_dense_rank_2"),
        F.round(F.avg("credit_limit_dense_rank"), 2).alias("avg_credit_limit_dense_rank"),
        F.min("browser_count_credit_limit_percent_rank").alias("min_browser_count_credit_limit_percent_rank"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
)

result = (
    totals
    .select(
        "browser",
        "total_credit_limit_percent_rank_dense_rank",
        "total_credit_limit_percent_rank_dense_rank_2",
        "avg_credit_limit_dense_rank",
        "min_browser_count_credit_limit_percent_rank",
    )
    .orderBy(F.col("avg_credit_limit_dense_rank").desc())
)
