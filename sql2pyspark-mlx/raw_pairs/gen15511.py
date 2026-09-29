from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("starts_on"), "promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w2 = Window.partitionBy(F.month("ends_on"), "promo_code").orderBy("discount_pct_whole", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_whole").asc_nulls_last())
w4 = Window.partitionBy("channel", "promo_code").orderBy("promo_id_avg_6", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy(F.col("row_seq").desc_nulls_first())
w6 = Window.partitionBy("channel", "promo_code").orderBy("last_known_row_seq", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel")
w8 = Window.partitionBy("promo_code")
w9 = Window.partitionBy("channel").orderBy(F.col("discount_pct_whole_percent_rank").desc_nulls_last(), "id", "promo_id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.floor("discount_pct").alias("discount_pct_whole"),
    )
)

with_window = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    with_window
    .filter(F.col("promotion_promo_code").isin(["BUNDLE", "SPRING10", "VIP"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct_whole",
        "promo_id",
        F.row_number().over(w).alias("row_seq"),
        F.avg("promo_id").over(w2).alias("promo_id_avg_6"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.percent_rank().over(w3).alias("discount_pct_whole_percent_rank"),
        F.last("row_seq", ignorenulls=True).over(w4).alias("last_known_row_seq"),
        F.rank().over(w5).alias("row_seq_rank"),
    )
)

metrics_promotions = (
    metrics
    .select(
        "*",
        F.sum("promo_id_avg_6").over(w6).alias("cumulative_promo_id_avg_6"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w7)), 4).alias("row_seq_share"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w8)), 4).alias("row_seq_share_2"),
    )
)

ranked = (
    metrics_promotions
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "channel",
        "discount_pct_whole",
        "promo_id",
        "promo_id_avg_6",
        "discount_pct_whole_percent_rank",
        "row_seq_rank",
        "row_seq_share",
        "row_seq_share_2",
    )
)

cleaned_promotions = (
    ranked
    .filter(F.col("promo_id_avg_6").between(4, 6))
    .select("*", F.coalesce("row_seq_rank", F.lit(0)).alias("row_seq_rank_filled"))
)

result = (
    cleaned_promotions
    .select(
        "id",
        "promo_id",
        "channel",
        "discount_pct_whole_percent_rank",
        "row_seq_share_2",
    )
    .unpivot(
        ids=["id", "promo_id", "channel"],
        values=["discount_pct_whole_percent_rank", "row_seq_share_2"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)
