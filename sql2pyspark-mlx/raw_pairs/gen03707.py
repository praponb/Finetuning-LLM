from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy(F.month("signup_date"), "country")
w5 = Window.partitionBy("account_account_type", "city").orderBy("signup_date", "id")
w6 = Window.partitionBy("city", "segment").orderBy(F.col("signup_date").desc(), "id")

latest_match = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.account_type").alias("account_account_type"),
        F.col("c.opened_date").alias("account_opened_date"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.account_opened_date")), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.start_date").alias("subscription_start_date"),
        F.col("c.monthly_fee"),
    )
)

scored = (
    with_last
    .select(
        "*",
        F.ntile(10).over(w3).alias("monthly_fee_bucket"),
        F.min("monthly_fee").over(w4).alias("signup_date_month_min_monthly_fee"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("seq_no"),
    )
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
        "is_active",
        "credit_limit",
        F.when(F.col("name").rlike("^(Noah Park|Kanya Srisuk)$"), "GROUP_A").otherwise("CORE").alias("name_zone"),
    )
)
