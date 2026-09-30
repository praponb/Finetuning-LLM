from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy(F.date_trunc("YEAR", "opened_date")).orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

valid = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.concat("account_type", F.lit("/"), "status").alias("account_type_status"),
    )
)

enriched = (
    valid
    .filter(F.col("status").isin(["FROZEN", "OPEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        "account_type_status",
        F.avg("balance").over(w).alias("balance_avg_2_prior"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
    )
)

remaining_keys = (
    enriched
    .select("account_type", "id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isin([3, 7, 36]))
        .select("account_type", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("status").eqNullSafe(F.lit("FROZEN")))
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance_avg_2_prior",
        "cumulative_balance",
        F.concat_ws("/", "status", "account_type").alias("status_account_type_key"),
        F.concat_ws(",", "status", "account_type").alias("status_account_type_key_2"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.balance_avg_2_prior",
        "a.cumulative_balance",
        F.col("d.age"),
        F.col("d.signup_date"),
        F.col("d.name"),
    )
)

result = (
    joined
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance_avg_2_prior",
        "cumulative_balance",
        "name",
        F.when(F.col("name").rlike("^(Noah Park|Emma Brown)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("name_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
)
