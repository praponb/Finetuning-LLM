from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

attributed = (
    df_orders
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "GROUP_A").otherwise("GROUP_B").alias("promo_code_zone"),
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("promo_code").desc_nulls_last(), F.col("promo_id").desc())
)
