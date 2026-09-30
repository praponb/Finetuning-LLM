from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("credit_limit_minus_age", "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("credit_limit_minus_age").desc())
w_grp = Window.partitionBy("city").orderBy("age", "id")
w4 = Window.partitionBy("country").orderBy(F.col("credit_limit_minus_age_bucket").desc(), "id")
w5 = Window.partitionBy("country").orderBy(F.col("age_bucket").desc())
w6 = Window.partitionBy("country").orderBy(F.col("age_bucket").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-03-10").cast("date")) & F.col("email").like("%@startup.io"))
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "age",
        (F.col("credit_limit") - F.col("age")).alias("credit_limit_minus_age"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "country",
        F.ntile(10).over(w).alias("credit_limit_minus_age_bucket"),
        F.rank().over(w2).alias("credit_limit_minus_age_rank"),
        F.ntile(3).over(w_grp).alias("age_bucket"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.ntile(3).over(w4).alias("credit_limit_minus_age_bucket_bucket"),
        F.rank().over(w5).alias("age_bucket_rank"),
        F.ntile(3).over(w6).alias("age_bucket_bucket"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.balance").between(59000, 66000)), "left_anti")
)

cleaned_customers = (
    eligible
    .filter(F.col("age_bucket") <= 7)
    .select(
        "id",
        "credit_limit_minus_age_bucket",
        "credit_limit_minus_age_rank",
        "age_bucket",
        "credit_limit_minus_age_bucket_bucket",
        "age_bucket_rank",
        "age_bucket_bucket",
        F.round((F.col("credit_limit_minus_age_bucket") * 1.1), 2).alias("adjusted_credit_limit_minus_age_bucket"),
    )
)

with_last = (
    cleaned_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.end_date").alias("subscription_end_date"), F.col("c.plan"))
)

result = (
    with_last
    .select(
        "id",
        "credit_limit_minus_age_rank",
        "age_bucket",
        "age_bucket_rank",
        "adjusted_credit_limit_minus_age_bucket",
    )
    .orderBy(F.col("age_bucket").desc_nulls_first(), "age_bucket_rank")
)
