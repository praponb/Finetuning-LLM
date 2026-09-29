from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("least_shipping_fee_amount").desc_nulls_last(), "id")

valid = (
    df_orders
    .filter(F.col("shipping_fee") > 22)
    .select(
        "*",
        F.least("shipping_fee", "amount").alias("least_shipping_fee_amount"),
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

latest = (
    latest_match
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

matched = (
    latest
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

totals = (
    matched
    .groupBy("promo_code")
    .agg(
        F.percentile_approx("least_shipping_fee_amount", 0.5).alias("least_shipping_fee_amount_p50"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
)

result = (
    totals
    .select(
        "promo_code",
        "partner_rows",
        "num_promo_codes",
        F.when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "PRIORITY").otherwise("EXTENDED").alias("promo_code_zone"),
    )
)
