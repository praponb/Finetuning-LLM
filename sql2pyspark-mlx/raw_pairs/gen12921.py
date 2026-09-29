from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

joined = (
    flattened
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.signup_date") < "2023-03-01")
    .select(
        "e.id",
        "e.customer_id",
        "e.k",
        "e.v",
        F.col("d.city"),
        F.col("d.signup_date"),
    )
)

joined_carts = (
    joined
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.signup_date").alias("customer_signup_date"))
)

joined_carts_2 = (
    joined_carts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "a.*",
        F.col("p.credit_limit"),
        F.col("p.signup_date").alias("customer_signup_date_2"),
        F.col("p.email"),
    )
)

result = (
    joined_carts_2
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "k",
        "v",
        "signup_date",
        "credit_limit",
        "customer_signup_date_2",
        "email",
        F.col("max_account_balance"),
    )
    .orderBy(F.col("max_account_balance").desc())
)
