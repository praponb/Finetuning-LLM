from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

joined = (
    exploded
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.email").like("%@company.co"))
    .select("s.id", "s.customer_id", "s.code", F.col("d.segment"))
)

remaining_keys = (
    joined
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("id", "customer_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.country"),
        F.col("p.city"),
        F.col("p.segment").alias("customer_segment"),
    )
)

result = (
    with_dims
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "code",
        "segment",
        "customer_segment",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("min_account_balance").desc(), F.col("id").desc())
)
