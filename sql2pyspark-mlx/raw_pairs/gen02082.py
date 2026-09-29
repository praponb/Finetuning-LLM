from pyspark.sql import functions as F

quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

matched = (
    df_orders
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "promo_id",
        F.when(F.col("channel").rlike("^(partner|mobile)$"), "CORE").otherwise("EXTENDED").alias("channel_zone"),
        F.col("max_inventory_quantity"),
    )
    .orderBy("max_inventory_quantity", "channel")
)
