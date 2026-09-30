from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

enriched = (
    exploded
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        F.col("c.signup_date"),
        F.col("c.name"),
        F.col("c.email"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.name").alias("customer_name"),
        F.col("p.email").alias("customer_email"),
        F.col("p.age"),
    )
)

joined_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.city").isin(["Berlin", "London"]))
    .select(
        "s.id",
        "s.customer_id",
        "s.signup_date",
        "s.name",
        "s.email",
        "s.customer_name",
        "s.customer_email",
        "s.age",
        F.col("p.credit_limit"),
        F.col("p.country"),
    )
)

result = (
    joined_carts
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "name",
        "email",
        "customer_name",
        "customer_email",
        "age",
        "credit_limit",
        F.when(F.col("customer_name").rlike("^(Alice Wong|David Lee )$"), "GROUP_B").when(F.col("customer_name").rlike("^(bob smith|  Carol Diaz)$"), "CORE").otherwise("EXTENDED").alias("customer_name_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
