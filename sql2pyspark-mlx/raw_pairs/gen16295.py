from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_last())

base = (
    df_customers
    .filter(F.col("age") > 35)
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.ceil("age").alias("age_whole"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.start_date") >= F.col("s.signup_date")), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-07-17")))
    .select("*", F.substring("country", 1, 3).alias("country_prefix"))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_anti")
)

stats = (
    not_flagged
    .groupBy(F.date_trunc("year", "signup_date").alias("year_start"), "country")
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("credit_limit_per_uk"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)

result = (
    stats
    .select(
        "year_start",
        "country",
        "credit_limit_per_uk",
        "num_countries",
        F.when(F.col("country").rlike("^(TH|DE)$"), "EXTENDED").when(F.col("country").rlike("^(US|SG)$"), "CORE").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("num_countries").desc())
)
