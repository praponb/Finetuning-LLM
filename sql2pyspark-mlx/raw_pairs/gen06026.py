from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id", "promo_id")

base = (
    df_promotions
    .filter(~F.col("channel").eqNullSafe(F.lit("web")))
    .select(
        "*",
        F.concat("promo_code", F.lit(" "), "channel").alias("promo_code_channel"),
    )
)

attributed = (
    base
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    attributed
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
    )
)

kept_keys = (
    latest
    .select("promo_code")
    .intersect(
        df_promotions
        .filter(F.col("discount_pct") < 0.12)
        .select("promo_code")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

base_promotions = (
    kept_rows
    .filter((F.col("ends_on") >= F.lit("2023-05-23").cast("date")) & (F.col("discount_pct") >= 0.24))
    .withColumn("promo_code_group", F.when(F.col("promo_code").isin(["WELCOME5", "VIP"]), "Priority").otherwise("Other"))
)

result = (
    base_promotions
    .select(
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "promo_code_group",
        F.when(F.col("promo_code").rlike("^(WELCOME5|VIP)$"), "CORE").when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "GROUP_A").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("discount_pct").asc_nulls_first(), F.col("promo_id").desc())
)
