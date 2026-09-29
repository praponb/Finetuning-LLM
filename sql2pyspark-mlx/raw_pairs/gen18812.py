from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") >= "2024-02-01"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.lower("status").alias("status_lower"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.status",
        F.col("b.amount").alias("order_amount"),
        F.col("b.discount"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.amount") >= 1500)
    .select(
        "a.id",
        "a.order_id",
        "a.amount",
        "a.status",
        "a.discount",
        F.col("p.shipping_fee"),
        F.col("p.order_date"),
    )
)

enriched_payments = (
    with_dims
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id").eqNullSafe(F.col("o.id")), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.status",
        "e.order_date",
        F.col("o.status").alias("order_status"),
        F.col("o.discount").alias("order_discount"),
        F.col("o.order_date").alias("order_order_date"),
    )
)

with_dims_payments = (
    enriched_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.order_date",
        "e.order_status",
        "e.order_order_date",
        F.col("p.channel"),
    )
)

matched = (
    with_dims_payments
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "order_id",
        "amount",
        "order_date",
        "order_status",
        "order_order_date",
        "promo_id",
        "promo_code",
    )
    .orderBy(F.col("amount").desc())
)
