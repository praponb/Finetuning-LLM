from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy(F.year("order_date"), "store_id").orderBy(F.col("promo_id").asc_nulls_first())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

latest_match = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

with_window = (
    latest_match
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

remaining_keys = (
    with_window
    .select("status")
    .subtract(
        df_orders
        .filter(F.col("amount").between(1270, 1300))
        .select("status")
    )
)

reconciled = (
    with_window
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.year(F.col("ch.shipped_date")) >= 2024), "left_anti")
)

metrics = (
    kept
    .withColumn("status_sum_shipping_fee", F.sum("shipping_fee").over(w2))
    .withColumn("promo_id_cume_dist", F.cume_dist().over(w3))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price") >= 230), "left_semi")
)

attributed = (
    not_flagged
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.when(F.col("channel").rlike("^(partner|web)$"), "CORE").when(F.col("channel").rlike("^(mobile|store)$"), "PRIORITY").otherwise("EXTENDED").alias("channel_zone"),
        F.col("avg_inventory_quantity"),
    )
    .orderBy("promo_id", F.col("promo_code").desc())
)
