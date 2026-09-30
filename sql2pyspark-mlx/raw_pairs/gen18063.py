from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

cleaned = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.store_id").isNotNull())
    .select(
        "src.*",
        F.col("d.channel"),
        F.col("d.shipping_fee"),
        F.col("d.amount").alias("order_amount"),
    )
)

kept_keys = (
    with_dims
    .select("method")
    .subtract(
        df_payments
        .filter(F.col("paid_at") < "2025-04-13")
        .select("method")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

with_window = (
    reconciled
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.paid_at").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = with_window.withColumn("amount_cume_dist", F.cume_dist().over(w_ordered))

result = (
    scored
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "status_group",
        "method_clean",
        "channel",
        "shipping_fee",
        "order_amount",
        F.col("max_shipment_cost"),
    )
)
