from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date")).orderBy(F.col("age").desc())
w2 = Window.partitionBy("city", "segment")
w3 = Window.partitionBy("country", "city")
w4 = Window.partitionBy("city").orderBy("age_percent_rank", "id")
w5 = Window.orderBy("signup_date", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("segment", "country").orderBy(F.col("credit_limit_share").desc())
w7 = Window.partitionBy("segment").orderBy("credit_limit_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy(F.date_trunc("month", "signup_date"), "country").orderBy("credit_limit_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w11 = Window.partitionBy("country").orderBy(F.col("credit_limit_share").desc_nulls_first(), "id")
w12 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w13 = Window.partitionBy("country").orderBy(F.col("credit_limit_share").desc_nulls_last())
w14 = Window.partitionBy("city")

scored = (
    df_customers
    .filter(F.col("signup_date") <= "2024-09-01")
    .withColumn("age_percent_rank", F.percent_rank().over(w))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w3))
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "country",
        "city",
        "is_active",
        "age",
        "credit_limit_share",
        F.avg("credit_limit").over(w5).alias("credit_limit_avg_2_prior"),
        F.cume_dist().over(w6).alias("credit_limit_share_cume_dist"),
        F.first("age_percent_rank", ignorenulls=True).over(w7).alias("first_known_age_percent_rank"),
        F.sum("corr_age_credit_limit").over(w_running).alias("running_corr_age_credit_limit"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.monthly_fee"), F.col("c.end_date"))
)

with_last_customers = (
    with_last
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.is_active",
        "s.age",
        "s.credit_limit_share",
        F.col("c.status"),
    )
)

latest_customers = (
    with_last_customers
    .withColumn("row_num", F.row_number().over(w11))
    .filter(F.col("row_num") == 1)
)

with_last_customers_2 = (
    latest_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("subscription_status"))
)

result = (
    with_last_customers_2
    .select(
        "*",
        F.percent_rank().over(w13).alias("credit_limit_share_percent_rank"),
        F.round((F.col("age") / F.sum("age").over(w14)), 4).alias("age_share"),
    )
)
