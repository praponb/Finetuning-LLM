from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_percent_rank").desc_nulls_last(), "id")
w4 = Window.partitionBy("channel", "promo_code").orderBy(F.col("starts_on").desc(), "id")
w5 = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w6 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") < "2023-10-01"))
    .select(
        "*",
        F.date_format("starts_on", "yyyy-MM").alias("starts_on_label"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("discount_pct_percent_rank"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

latest_promotions = (
    latest
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_promotions_2 = (
    latest_promotions
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest_promotions_2
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

matched = (
    ranked
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "promo_id",
        "channel",
        "discount_pct_percent_rank",
        "promotion_discount_pct",
    )
    .unpivot(
        ["id", "promo_id", "channel"],
        ["discount_pct_percent_rank", "promotion_discount_pct"],
        "kpi",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
