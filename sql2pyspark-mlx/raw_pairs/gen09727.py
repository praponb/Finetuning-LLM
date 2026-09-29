from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "country")
w3 = Window.orderBy(F.col("credit_limit").desc_nulls_last())
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy(F.year("signup_date"), "country").orderBy("start_date", "id")
w7 = Window.partitionBy("city", "country").orderBy("corr_credit_limit_age", "id")
w8 = Window.partitionBy("city").orderBy("signup_date", "id")
w9 = Window.partitionBy(F.year("signup_date"), "country")
w10 = Window.partitionBy(F.date_trunc("month", "start_date"), "country").orderBy("start_date", "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-06-15").cast("date")) & F.col("city").isNotNull())
    .select("*", (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"))
)

with_last = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        F.col("c.start_date"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.balance") > 46000), "left_anti")
)

enriched = (
    kept
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w2))
    .withColumn("credit_limit_rank", F.rank().over(w3))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w4))
    .withColumn("credit_limit_avg_5_prior", F.avg("credit_limit").over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

metrics = (
    enriched
    .select(
        "id",
        "country",
        "signup_date",
        "credit_limit",
        "age",
        "corr_credit_limit_age",
        "credit_limit_rank",
        "corr_age_credit_limit",
        "credit_limit_avg_5_prior",
        "event_seq",
        F.lead("event_seq", 1).over(w7).alias("next_event_seq"),
        F.lead("event_seq", 1).over(w8).alias("next_event_seq_2"),
        F.round((F.col("credit_limit_rank") / F.sum("credit_limit_rank").over(w9)), 4).alias("credit_limit_rank_share"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "country",
        "signup_date",
        "credit_limit",
        "age",
        "corr_credit_limit_age",
        "corr_age_credit_limit",
        "credit_limit_avg_5_prior",
        "event_seq",
    )
)
