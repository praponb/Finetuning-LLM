from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "channel").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w3 = Window.partitionBy("channel").orderBy(F.col("running_min_discount_pct").asc_nulls_first(), "id", "promo_id")
w_part = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id").rowsBetween(-6, -1)
w5 = Window.partitionBy("channel").orderBy("months_ends_on_to_starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("starts_on") < "2025-03-23")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        F.round((F.datediff("starts_on", "ends_on") / 30), 1).alias("months_ends_on_to_starts_on"),
    )
)

with_window = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_windows = (
    with_window
    .select(
        "id",
        "channel",
        "starts_on",
        "months_ends_on_to_starts_on",
        "promo_id",
        F.count("discount_pct").over(w).alias("running_count_discount_pct"),
        F.min("discount_pct").over(w).alias("running_min_discount_pct"),
        F.avg("discount_pct").over(w).alias("running_avg_discount_pct"),
        F.max("discount_pct").over(w).alias("running_max_discount_pct"),
        F.ntile(5).over(w2).alias("promo_id_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "promo_id",
        "running_min_discount_pct",
        F.avg("running_min_discount_pct").over(w_part).alias("running_min_discount_pct_avg_6_prior"),
        F.last("running_count_discount_pct", ignorenulls=True).over(w5).alias("last_known_running_count_discount_pct"),
    )
)

result = (
    metrics
    .select(
        "id",
        "promo_id",
        "running_min_discount_pct",
        "running_min_discount_pct_avg_6_prior",
        "last_known_running_count_discount_pct",
    )
    .orderBy("id")
)
