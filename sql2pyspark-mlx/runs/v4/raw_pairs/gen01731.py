from pyspark.sql import functions as F

with_window = (
    df_orders
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    with_window
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
