from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("store"))) & (F.col("order_date") > F.lit("2023-09-14").cast("date")))
    .withColumn("channel_status", F.concat("channel", F.lit(" "), "status"))
)

result = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)
