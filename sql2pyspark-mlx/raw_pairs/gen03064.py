from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "signup_date"), "city")
w2 = Window.partitionBy(F.month("signup_date"))
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

joined = (
    exploded
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("c.signup_date"),
    )
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.credit_limit",
        "e.city",
        "e.signup_date",
        F.col("p.city").alias("customer_city"),
        F.col("p.signup_date").alias("customer_signup_date"),
        F.col("p.age"),
    )
)

enriched = (
    with_dims
    .filter(~F.col("customer_city").isin(["Tokyo", "Bangkok"]))
    .select("*", F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"))
)

scored = (
    enriched
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
    )
)

result = (
    scored
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "city",
        "signup_date",
        "customer_signup_date",
        "age",
        "credit_limit_share",
        F.col("min_account_balance"),
    )
)
