from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(~F.col("channel").eqNullSafe(F.lit("mobile")))
    .select(
        "*",
        F.date_format("order_date", "MMM yyyy").alias("order_date_label"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)
