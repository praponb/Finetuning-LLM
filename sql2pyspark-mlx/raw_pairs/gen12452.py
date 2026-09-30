from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "paid_at"), "order_id")
w2 = Window.partitionBy(F.to_date("paid_at"), "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-10-01").cast("date")) & (F.col("amount") <= 1560))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.hour("paid_at").alias("paid_at_hour"),
        F.when(F.col("amount") >= 1410, "High").when(F.col("amount") >= 710, "Medium").otherwise("Low").alias("amount_band"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.shipping_fee"), F.col("p.order_date"), F.col("p.channel"))
)

joined = (
    enriched
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "inner")
    .filter(F.col("o.customer_id") == 15)
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.paid_at",
        "a.paid_at_hour",
        "a.shipping_fee",
        "a.channel",
        F.col("o.shipping_fee").alias("order_shipping_fee"),
    )
)

kept_keys = (
    joined
    .select("method", "order_id")
    .subtract(
        df_payments
        .filter(F.to_date("paid_at") < "2024-06-10")
        .select("method", "order_id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

attributed = (
    kept_rows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.paid_at") >= F.col("pr.starts_on")) & (F.col("s.paid_at") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    attributed
    .select(
        "id",
        "order_id",
        "paid_at_hour",
        "shipping_fee",
        "order_shipping_fee",
        "promo_id",
        "promo_code",
        F.sum("shipping_fee").over(w).alias("paid_at_period_sum_shipping_fee"),
        F.sum("shipping_fee").over(w2).alias("shipping_fee_sum_30d"),
    )
)

result = (
    metrics
    .select("order_id", "order_shipping_fee", "shipping_fee")
    .unpivot(
        ids=["order_id"],
        values=["order_shipping_fee", "shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
