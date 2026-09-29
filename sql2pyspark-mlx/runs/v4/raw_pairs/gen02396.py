from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-10-30").cast("date")) & F.col("carrier").isNotNull())
    .select("*", F.ceil("cost").alias("cost_whole"))
)

enriched = (
    filtered
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.channel"))
)

matched = (
    enriched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.shipped_date") >= F.col("pr.starts_on")) & (F.col("src.shipped_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("cost") >= 78, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("promo_id", [0.25, 0.5, 0.75]).alias("promo_id_quartiles"),
    )
    .orderBy(F.col("cost_tier").desc_nulls_first())
)
