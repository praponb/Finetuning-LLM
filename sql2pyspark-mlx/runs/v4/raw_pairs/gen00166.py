from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct")

with_windows = (
    df_promotions
    .select(
        "*",
        F.ntile(3).over(w).alias("discount_pct_bucket"),
        F.percent_rank().over(w2).alias("discount_pct_percent_rank"),
    )
)

remaining_keys = (
    with_windows
    .select("channel")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct").between(0.2, 0.22))
        .select("channel")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter((F.col("starts_on") > F.lit("2024-02-29").cast("date")) & F.col("channel").isNotNull() & (F.col("discount_pct") < 0.32))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct_bucket",
        "discount_pct_percent_rank",
        F.floor("discount_pct_percent_rank").alias("discount_pct_percent_rank_whole"),
    )
)

filtered = (
    cleaned
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct_percent_rank") >= 0.4))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct_bucket",
        "discount_pct_percent_rank",
        F.greatest("discount_pct_percent_rank", "discount_pct_percent_rank_whole").alias("greatest_discount_pct_percent_rank_discount_pct_percent_rank_whole"),
    )
)

with_window = (
    filtered
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    with_window
    .cube(
        "promo_id",
        F.date_trunc("year", "starts_on").alias("year_start"),
        F.date_trunc("month", "ends_on").alias("activity_month"),
    )
    .agg(
        F.max("greatest_discount_pct_percent_rank_discount_pct_percent_rank_whole").alias("max_greatest_discount_pct_percent_rank_discount_pct_percent_rank_whole"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
        F.count("*").alias("row_count"),
    )
    .orderBy("promo_id")
)
