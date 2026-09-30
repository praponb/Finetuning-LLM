from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "promotion_promo_code").orderBy(F.col("promo_id_2").desc_nulls_last(), "id", "promo_id", "promo_id_2")
w2 = Window.partitionBy("promo_code")

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-04").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)

attributed = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

attributed_orders = (
    attributed
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

cleaned = (
    attributed_orders
    .filter(F.col("order_date") > F.lit("2023-03-16").cast("date"))
    .select(
        "*",
        F.col("promotion_discount_pct").cast("int").alias("promotion_discount_pct_int"),
        F.ceil("shipping_fee").alias("shipping_fee_whole"),
        F.coalesce("promo_code", F.lit("DIRECT")).alias("promo_code_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "amount",
        "discount_filled",
        "promo_id",
        "promo_code",
        "promo_id_2",
        "promotion_discount_pct",
        "promo_code_filled",
    )
)

kept_keys = (
    ranked
    .select("id")
    .intersect(
        df_orders
        .filter(~F.col("status").isin(["COMPLETED", "CANCELLED", "PENDING"]))
        .select("id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "id",
        "amount",
        F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w2)), 4).alias("promotion_discount_pct_share"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)

result = (
    enriched
    .select("amount", "amount_share")
    .orderBy("amount_share")
)
