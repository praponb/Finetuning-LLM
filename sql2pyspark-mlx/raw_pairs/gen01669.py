from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w_part = Window.partitionBy("country").orderBy(F.col("is_us").desc_nulls_last())
w3 = Window.partitionBy("segment", "city")

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-08-08").cast("date")) & (F.col("city") != "Bangkok"))
    .select("*", F.regexp_replace("name", " ", "").alias("name_slug"))
)

kept_keys = (
    filtered
    .select("segment")
    .subtract(
        df_customers
        .filter(F.col("country").isin(["US", "TH", "JP"]))
        .select("segment")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("city").isNotNull() & F.col("city").isin(["Bangkok", "Tokyo", "London"]))
    .withColumn("is_us", F.when(F.col("country") == "US", 1).otherwise(0))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date").between("2024-03-11", "2024-11-19")), "left_semi")
)

metrics = (
    not_flagged
    .select(
        "id",
        "email",
        "country",
        "is_active",
        "credit_limit",
        "age",
        "name_slug",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.cume_dist().over(w_part).alias("is_us_cume_dist"),
        F.corr("credit_limit", "age").over(w3).alias("corr_credit_limit_age_2"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("is_us_cume_dist") >= 0.8, "LARGE").otherwise("SMALL").alias("is_us_cume_dist_tier"),
    )
    .agg(
        F.min("corr_credit_limit_age").alias("min_corr_credit_limit_age"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("country") == "DE", F.col("is_us_cume_dist"))).alias("de_total_is_us_cume_dist"),
    )
    .filter(F.col("num_countries") < 1000)
    .orderBy(F.col("num_countries").desc())
)
