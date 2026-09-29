from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc())

base = (
    df_promotions
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-11-09")))
    .withColumn("discount_pct_whole", F.floor("discount_pct"))
    .withColumn("days_starts_on_to_ends_on", F.datediff("ends_on", "starts_on"))
)

latest = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

attributed = (
    latest
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched = (
    with_window
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    matched
    .groupBy("promotion_promo_code_3", "promotion_promo_code")
    .pivot("promotion_promo_code_2", ["BUNDLE", "WELCOME5"])
    .agg(F.sum("discount_pct"))
    .select(
        "promotion_promo_code_3",
        "promotion_promo_code",
        F.col("BUNDLE").alias("bundle"),
        F.col("WELCOME5").alias("welcome5"),
    )
)
