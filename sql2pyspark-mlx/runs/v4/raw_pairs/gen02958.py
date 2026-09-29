from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct_rounded").desc(), "id")
w_ordered = Window.orderBy("ends_on", "id", "promo_id")
w3 = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel")
w_running = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("promotion_discount_pct", "id", "promo_id")
w6 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("discount_pct_rounded").desc(), "id", "promo_id")
w7 = Window.partitionBy("channel", "promotion_promo_code").orderBy("discount_pct_rounded", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code").orderBy("promotion_discount_pct_share", "id", "promo_id").rowsBetween(-5, Window.currentRow)

base = (
    df_promotions
    .filter(~F.col("channel").isin(["partner", "store"]))
    .select(
        "*",
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

matched = (
    latest
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    matched
    .select(
        "*",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.sum("promo_id").over(w3).alias("cumulative_promo_id"),
        F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w4)), 4).alias("promotion_discount_pct_share"),
        F.ntile(5).over(w_running).alias("promotion_discount_pct_bucket"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.row_number().over(w6).alias("row_seq"),
        F.first("cumulative_promo_id", ignorenulls=True).over(w7).alias("first_known_cumulative_promo_id"),
        F.avg("seq_no").over(w8).alias("seq_no_avg_5"),
    )
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        "discount_pct_rounded",
        "resolved_channel",
        "row_num",
        "promo_id",
        "promotion_promo_code",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "EXTENDED").when(F.col("channel").rlike("^(store|partner)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("channel_zone"),
    )
)
