from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("line_total", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").isNull())
    .select(
        "*",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "product_id", "quantity")
)

with_dims = (
    deduped
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.id", "s.order_id", "s.product_id", F.col("p.amount"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        F.col("b.shipping_fee"),
        F.col("b.order_date"),
        F.col("b.channel"),
    )
)

matched = (
    joined
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_dims_order_items = (
    matched
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.discount"))
)

result = (
    with_dims_order_items
    .groupBy(
        F.when(F.col("promo_id") >= 8, "HIGH").otherwise("LOW").alias("promo_id_tier"),
        "promo_code",
        F.when(F.col("shipping_fee") >= 39, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("discount_pct")).otherwise(0)).alias("vip_discount_pct"),
    )
)
