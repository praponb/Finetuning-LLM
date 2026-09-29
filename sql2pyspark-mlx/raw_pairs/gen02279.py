from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

base = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED"]) & (F.col("opened_date") >= F.to_date(F.lit("2023-07-03"))))
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 1))
    .withColumn("account_type_status", F.concat("account_type", F.lit(" "), "status"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("DIRECT")))
)

not_flagged = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date_plus_months")), "left_semi")
)

kept_keys = (
    not_flagged
    .select("status")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

joined = (
    eligible
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.customer_id", "src.balance", "src.opened_date", F.col("d.age"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "s.customer_id",
        "s.balance",
        "s.age",
        F.col("c.segment"),
        F.col("c.age").alias("customer_age"),
        F.col("c.city"),
    )
)

result = (
    with_dims
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "age",
        "segment",
        "customer_age",
        "city",
        F.when(F.col("city").rlike("^(Singapore|Tokyo)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
)
