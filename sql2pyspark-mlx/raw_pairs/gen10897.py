from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-05-27").cast("date")) & (F.col("amount") >= 1200))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "discount",
        "shipping_fee",
        F.add_months("order_date", 6).alias("order_date_plus_months"),
    )
)

attributed = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.order_date_plus_months").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept = (
    attributed
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([26, 35])), "left_anti")
)

matched_keys = (
    kept
    .select("customer_id", "id")
    .intersectAll(
        df_orders
        .filter(F.col("amount") >= 1570)
        .select("customer_id", "id")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base_orders = (
    kept_rows
    .filter(F.col("plan").isNotNull() & F.col("monthly_fee").between(70, 230))
    .select(
        "*",
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
        F.coalesce("plan", F.lit("DIRECT")).alias("resolved_plan"),
    )
)

kept_orders = (
    base_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") > 6), "left_semi")
)

joined = (
    kept_orders
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.store_name").like("Old%"))
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.order_date",
        "src.discount",
        "src.shipping_fee",
        "src.order_date_plus_months",
        "src.subscription_id",
        "src.plan",
        "src.resolved_plan",
        F.col("b.sqft"),
    )
)

result = (
    joined
    .select("customer_id", "discount", "shipping_fee")
    .unpivot(
        ids=["customer_id"],
        values=["discount", "shipping_fee"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
