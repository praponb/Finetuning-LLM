from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-09-14").cast("date")) & (F.col("shipping_fee") < 15))
    .withColumn("years_since_order_date", (F.datediff(F.current_date(), "order_date") / 365))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

kept_keys = (
    cleaned
    .select("store_id")
    .intersect(
        df_orders
        .filter(F.col("channel") != "store")
        .select("store_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

attributed = (
    verified
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

attributed_orders = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

base = (
    attributed_orders
    .filter((F.col("order_date") > F.lit("2023-05-17").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "years_since_order_date",
        "promo_id",
        "promo_code",
        "discount_pct",
        "promo_id_2",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.substring("promo_code", 1, 3).alias("promo_code_prefix"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

filtered = (
    base
    .filter(F.col("promo_code").isin(["SPRING10", "VIP"]))
    .select(
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "years_since_order_date",
        "promo_id",
        "promo_code",
        "promotion_promo_code",
        F.when(F.col("status") == "PENDING", 1).otherwise(0).alias("is_pending"),
    )
)

base_orders = (
    filtered
    .filter(~F.col("promotion_promo_code").eqNullSafe(F.lit("BUNDLE")))
    .withColumn("amount_rounded", F.round("amount", 0))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("resolved_promo_code", F.coalesce("promo_code", F.lit("DIRECT")))
)

result = (
    base_orders
    .select(
        "promo_code",
        "amount",
        F.col("amount_rounded").alias("AMOUNT_ROUNDED_VAL"),
        F.col("discount").alias("DISCOUNT"),
        F.col("years_since_order_date").alias("YEARS_SINCE_ORDER_DATE"),
    )
    .unpivot(
        ids=["promo_code"],
        values=["amount", "AMOUNT_ROUNDED_VAL", "DISCOUNT", "YEARS_SINCE_ORDER_DATE"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
