from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_last(), "id")
w3 = Window.partitionBy(F.month("signup_date"), "city").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy(F.date_trunc("YEAR", "signup_date")).orderBy(F.col("age").desc(), "id")
w5 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy(F.month("signup_date"), "segment").orderBy("signup_date", "id")
w7 = Window.partitionBy(F.month("signup_date"), "city")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-03-02"))) & F.col("city").isNotNull())
    .select(
        "*",
        F.upper("email").alias("email_upper"),
        F.when(F.col("segment") == "SMB", 1).otherwise(0).alias("is_smb"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") <= F.col("s.signup_date")), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

deduped = (
    with_last
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
)

not_flagged_customers = (
    deduped
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") <= F.col("s.signup_date")), "left_anti")
)

scored = (
    not_flagged_customers
    .select(
        "*",
        F.rank().over(w3).alias("credit_limit_rank"),
        F.ntile(10).over(w4).alias("age_bucket"),
        F.avg("credit_limit").over(w5).alias("credit_limit_avg_5_prior"),
        F.row_number().over(w6).alias("row_seq"),
        F.max("credit_limit").over(w7).alias("signup_date_month_max_credit_limit"),
    )
)

with_last_customers = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.age",
        "s.credit_limit_rank",
        "s.age_bucket",
        "s.row_seq",
        F.col("c.status").alias("account_status"),
        F.col("c.balance").alias("account_balance"),
    )
)

result = (
    with_last_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"), "city")
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.avg(F.when(F.col("account_status") == "CLOSED", F.col("age"))).alias("closed_avg_age"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("closed_avg_age") > 0)
)
