from pyspark.sql import functions as F

kept_keys = (
    df_orders
    .select("status")
    .subtract(
        df_orders
        .filter(F.col("order_date") <= "2024-07-01")
        .select("status")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status") == "IN_TRANSIT"), "left_anti")
)

attributed = (
    not_flagged
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

filtered = (
    attributed
    .filter((F.col("order_date") > F.lit("2023-07-08").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.greatest("discount", "promo_id").alias("greatest_discount_promo_id"),
        F.when(F.col("status").isin(["PENDING", "RETURNED"]), "Core").otherwise("Other").alias("status_group"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("discount_pct") >= 0.34, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.max("discount").alias("max_discount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
    )
    .orderBy(F.col("partner_events").desc_nulls_first())
)
