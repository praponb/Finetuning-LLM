from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "status").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("last_known_age_minus_balance", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("signup_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.credit_limit"), F.col("c.age"), F.col("c.city"))
)

enriched = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.country"), F.col("c.name"))
)

filtered = (
    enriched
    .filter(F.col("opened_date") > F.lit("2023-06-19").cast("date"))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "credit_limit",
        "age",
        "city",
        "country",
        "name",
        (F.col("age") - F.col("balance")).alias("age_minus_balance"),
        F.when(F.col("age") >= 73, "High").when(F.col("age") >= 35, "Medium").otherwise("Low").alias("age_level"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "customer_id",
        "status",
        "age",
        "city",
        "country",
        "name",
        "age_minus_balance",
        "age_level",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.last("age_minus_balance", ignorenulls=True).over(w2).alias("last_known_age_minus_balance"),
        F.last("age", ignorenulls=True).over(w3).alias("last_known_age"),
    )
)

joined_accounts = (
    with_windows
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.city").alias("customer_city"), F.col("b.signup_date"))
)

with_window = (
    joined_accounts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.signup_date") >= F.col("sub.start_date")) & (F.col("src.signup_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .select(
        "id",
        "city",
        "country",
        "age_minus_balance",
        "age_level",
        "last_known_country",
        "last_known_age_minus_balance",
        "last_known_age",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.sum("last_known_age_minus_balance").over(w4).alias("last4_sum_last_known_age_minus_balance"),
        F.count("last_known_age_minus_balance").over(w4).alias("last4_count_last_known_age_minus_balance"),
        F.max("last_known_age_minus_balance").over(w4).alias("last4_max_last_known_age_minus_balance"),
        F.sum("subscription_id").over(w5).alias("running_subscription_id"),
    )
)
