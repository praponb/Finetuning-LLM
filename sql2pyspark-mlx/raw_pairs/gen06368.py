from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc_nulls_last())
w2 = Window.partitionBy(F.date_trunc("MONTH", "signup_date"), "segment")
w3 = Window.orderBy("credit_limit")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy(F.month("signup_date"), "segment")

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.concat("city", F.lit("/"), "name").alias("city_name"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.signup_date")), "left_anti")
)

remaining_keys = (
    kept
    .select("segment", "city")
    .subtract(
        df_customers
        .filter(F.length("name") >= 6)
        .select("segment", "city")
    )
)

verified = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept_customers = (
    verified
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.start_date") < F.col("s.signup_date")), "left_semi")
)

scored = (
    kept_customers
    .select(
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "city_name",
        F.dense_rank().over(w).alias("age_dense_rank"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.cume_dist().over(w3).alias("credit_limit_cume_dist"),
    )
)

enriched = (
    scored
    .filter(F.col("credit_limit_cume_dist") < 0.28)
    .select(
        "city",
        "segment",
        "signup_date",
        "city_name",
        "age_dense_rank",
        F.avg("credit_limit_cume_dist").over(w4).alias("city_avg_credit_limit_cume_dist"),
        F.corr("credit_limit", "age").over(w4).alias("corr_credit_limit_age"),
        F.min("age_dense_rank").over(w5).alias("signup_date_month_min_age_dense_rank"),
    )
)

result = (
    enriched
    .select(
        "city",
        "segment",
        "signup_date",
        "city_name",
        "age_dense_rank",
        "city_avg_credit_limit_cume_dist",
        "corr_credit_limit_age",
        "signup_date_month_min_age_dense_rank",
    )
    .orderBy(F.col("signup_date").desc())
)
