from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc_nulls_first(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

latest = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "status")
)

joined = (
    latest
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.status",
        F.col("p.credit_limit"),
        F.col("p.signup_date"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.is_active"))
    .select("e.*", F.col("b.age"), F.col("b.name"), F.col("b.email"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.status",
        "e.credit_limit",
        "e.signup_date",
        "e.age",
        "e.email",
        F.col("b.credit_limit").alias("customer_credit_limit"),
        F.col("b.city"),
        F.col("b.signup_date").alias("customer_signup_date"),
    )
)

with_window = (
    with_dims
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.customer_signup_date") >= F.col("sub.start_date")) & (F.col("e.customer_signup_date") <= F.date_add(F.col("sub.end_date"), 3)), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "status",
        "credit_limit",
        "age",
        "email",
        "customer_credit_limit",
        "city",
        "customer_signup_date",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("credit_limit").desc())
)
