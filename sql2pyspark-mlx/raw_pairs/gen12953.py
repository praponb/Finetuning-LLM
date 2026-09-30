from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

joined = (
    lines
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.credit_limit") <= 6000)
    .select("src.*", F.col("p.country"), F.col("p.city"))
)

kept_keys = (
    joined
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.col("created_at") >= "2024-01-19")
        .select("id", "customer_id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (F.col("country").eqNullSafe(F.lit("SG"))))
    .select(
        "*",
        F.when(F.col("tag").isin(["SPRING10", "WELCOME5"]), "Priority").otherwise("Other").alias("tag_group"),
    )
)

result = (
    cleaned
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag",
        "tag_group",
        F.col("sum_order_amount"),
    )
    .orderBy("created_at")
)
