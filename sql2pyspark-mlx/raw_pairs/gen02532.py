from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy(F.to_date("created_at_period"), "code")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

joined = (
    lines
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.country") == "TH")
    .select("s.*", F.col("p.email"))
)

base = (
    joined
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-02 02:00:00"))) & F.col("email").like("%@outlook.com"))
    .select(
        "*",
        F.date_trunc("month", "created_at").alias("created_at_period"),
        F.weekofyear("created_at").alias("created_at_weekofyear"),
    )
)

with_windows = (
    base
    .select(
        "customer_id",
        "email",
        "created_at_weekofyear",
        F.avg("created_at_weekofyear").over(w).alias("created_at_weekofyear_avg_2h"),
        F.max("created_at_weekofyear").over(w2).alias("created_at_period_to_date_max_created_at_weekofyear"),
    )
)

report = (
    with_windows
    .select(
        "customer_id",
        "email",
        "created_at_weekofyear",
        "created_at_weekofyear_avg_2h",
        "created_at_period_to_date_max_created_at_weekofyear",
    )
    .orderBy("created_at_weekofyear_avg_2h")
)

result = (
    report
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "email",
        "created_at_weekofyear",
        "created_at_weekofyear_avg_2h",
        "created_at_period_to_date_max_created_at_weekofyear",
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("created_at_weekofyear").desc_nulls_first())
)
