from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

latest = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

matched = (
    latest
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

kept_keys = (
    matched
    .select("id", "customer_id")
    .intersect(
        df_orders
        .filter(F.col("status") == "COMPLETED")
        .select("id", "customer_id")
    )
)

reconciled = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter((F.col("order_date") >= F.lit("2023-06-24").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.last_day("order_date").alias("order_date_month_end"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

base = (
    valid
    .filter(F.col("customer_id").isNotNull() & F.col("amount").between(670, 750))
    .withColumn("promo_code_lower", F.lower("promo_code"))
    .withColumn("discount_times_amount", (F.col("discount") * F.col("amount")))
)

result = (
    base
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "order_date_month_end",
        "channel_filled",
        "promo_code_lower",
        F.col("avg_account_balance"),
    )
)
