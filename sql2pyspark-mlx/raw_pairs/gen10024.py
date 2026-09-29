from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city")
w2 = Window.partitionBy("country", "segment")
w3 = Window.partitionBy("segment")
w4 = Window.orderBy(F.col("corr_age_credit_limit").desc())
w_hist = Window.partitionBy("segment").orderBy(F.col("country_stddev_samp_credit_limit").desc_nulls_last())
w6 = Window.orderBy(F.col("corr_credit_limit_age").desc())

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("signup_date") > F.lit("2023-03-08").cast("date")))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("Unknown")))
    .withColumn("email_title", F.initcap(F.trim("email")))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

kept_keys = (
    base
    .select("country")
    .subtract(
        df_customers
        .filter(F.col("credit_limit") <= 9100)
        .select("country")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.stddev_samp("credit_limit").over(w2).alias("country_stddev_samp_credit_limit"),
        F.corr("age", "credit_limit").over(w3).alias("corr_age_credit_limit"),
    )
)

enriched_customers = (
    enriched
    .select(
        "name",
        "city",
        "segment",
        "credit_limit",
        "age",
        "segment_filled",
        "email_title",
        "resolved_city",
        "corr_credit_limit_age",
        "country_stddev_samp_credit_limit",
        F.cume_dist().over(w4).alias("corr_age_credit_limit_cume_dist"),
        F.dense_rank().over(w_hist).alias("country_stddev_samp_credit_limit_dense_rank"),
        F.rank().over(w6).alias("corr_credit_limit_age_rank"),
    )
)

result = (
    enriched_customers
    .select(
        "name",
        "segment",
        "age",
        "segment_filled",
        "email_title",
        "resolved_city",
        "corr_credit_limit_age",
        "country_stddev_samp_credit_limit",
        "corr_age_credit_limit_cume_dist",
    )
    .orderBy(F.col("segment").desc())
)
