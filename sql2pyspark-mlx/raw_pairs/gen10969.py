from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("ends_on")).orderBy("discount_pct", "id", "promo_id")
w_part = Window.partitionBy("promo_code", "promotion_promo_code").orderBy("discount_pct_whole", "id", "promo_id")
w3 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-03-24").cast("date")) & F.col("channel").isin(["mobile", "web"]))
    .withColumn("clean_promo_code", F.trim("promo_code"))
)

cleaned = (
    filtered
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-08-03"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "clean_promo_code",
        F.floor("discount_pct").alias("discount_pct_whole"),
    )
)

with_window = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched = (
    with_window
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "clean_promo_code",
        "discount_pct_whole",
        "promo_id",
        F.lag("discount_pct_whole", 1).over(w).alias("prev_discount_pct_whole"),
        F.ntile(5).over(w_part).alias("discount_pct_whole_bucket"),
        F.sum("promo_id").over(w3).alias("running_promo_id"),
    )
)

result = (
    enriched
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct_whole",
        "promo_id",
        "prev_discount_pct_whole",
        "discount_pct_whole_bucket",
        "running_promo_id",
        F.when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "GROUP_B").when(F.col("promo_code").rlike("^(VIP|BUNDLE)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("promo_code_zone"),
    )
)
