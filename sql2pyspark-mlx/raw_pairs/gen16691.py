from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("country")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

eligible = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") >= F.col("s.signup_date")), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "name",
        "email",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        F.avg("age").over(w).alias("age_avg_5_prior"),
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.balance"))
)

base = (
    latest_match
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "signup_date",
        "credit_limit",
        "age",
        "age_avg_5_prior",
        "corr_credit_limit_age",
        (F.col("age_avg_5_prior") * F.col("age")).alias("age_avg_5_prior_times_age"),
    )
)

report = (
    base
    .select("id", "name", "signup_date", "age")
    .orderBy("id")
    .limit(10)
)

result = report.select("id", "age")
