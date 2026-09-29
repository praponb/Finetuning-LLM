from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date_month_end"), "city").orderBy(F.col("weeks_signup_date_month_end_to_signup_date").desc_nulls_last())
w2 = Window.partitionBy("country", "segment")

kept_keys = (
    df_customers
    .select("country")
    .exceptAll(
        df_customers
        .filter(F.col("age") > 34)
        .select("country")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

cleaned = (
    base
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("London"))))
    .select(
        "*",
        (F.datediff("signup_date", "signup_date_month_end") / 7).alias("weeks_signup_date_month_end_to_signup_date"),
    )
)

scored = (
    cleaned
    .withColumn("weeks_signup_date_month_end_to_signup_date_dense_rank", F.dense_rank().over(w))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
)

result = (
    scored
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        "is_active_label",
    )
    .orderBy(F.col("city").desc_nulls_last())
)
