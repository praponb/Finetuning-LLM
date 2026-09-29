from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("opened_date") < "2024-04-28")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("customer_id", "id")
    .subtract(
        df_accounts
        .filter(F.col("account_type") == "CHECKING")
        .select("customer_id", "id")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.opened_date_month_end",
        F.col("p.name"),
        F.col("p.email"),
        F.col("p.age"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") < 35)
    .select(
        "src.customer_id",
        "src.name",
        "src.email",
        F.col("c.email").alias("customer_email"),
        F.col("c.name").alias("customer_name"),
        F.col("c.signup_date"),
    )
)

with_dims_accounts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.customer_id",
        "s.name",
        "s.email",
        "s.customer_name",
        "s.signup_date",
        F.col("b.segment"),
        F.col("b.age"),
        F.col("b.city"),
    )
)

result = (
    with_dims_accounts
    .groupBy(F.date_trunc("month", "signup_date").alias("activity_month"))
    .agg(
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("age_per_consumer"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
)
