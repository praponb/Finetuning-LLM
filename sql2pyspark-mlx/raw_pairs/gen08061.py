from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy(F.month("order_date"), "promo_code")
w2 = Window.partitionBy("store_id", "plan").orderBy(F.col("subscription_id").desc_nulls_first())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

matched = (
    df_orders
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.order_date") >= F.col("sub.start_date")) & (F.col("e.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

matched_orders = (
    matched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

kept_keys = (
    matched_orders
    .select("store_id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("status").isin(["CANCELLED", "PENDING"]))
        .select("store_id", "customer_id")
    )
)

kept_rows = (
    matched_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("status").isin(["RETURNED", "CANCELLED"]))
    .select(
        "*",
        F.avg("shipping_fee").over(w_grp).alias("order_date_month_avg_shipping_fee"),
        F.rank().over(w2).alias("subscription_id_rank"),
    )
)

result = (
    scored
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "shipping_fee",
        "subscription_id",
        "monthly_fee",
        "order_date_month_avg_shipping_fee",
        F.when(F.col("status").rlike("^(PENDING|COMPLETED)$"), "EXTENDED").when(F.col("status").rlike("^(CANCELLED|SHIPPED)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
        F.col("max_order_amount"),
    )
)
