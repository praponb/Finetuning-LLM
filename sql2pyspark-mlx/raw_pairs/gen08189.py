from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promotion_promo_code").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") >= "2024-07-25"))
    .withColumn("channel_promo_code_key", F.concat_ws("-", "channel", "promo_code"))
)

with_window = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    with_window
    .withColumn("running_max_promo_id", F.max("promo_id").over(w_grp))
    .withColumn("running_count_promo_id", F.count("promo_id").over(w_grp))
    .withColumn("running_sum_promo_id", F.sum("promo_id").over(w_grp))
)

base = (
    metrics
    .filter((F.col("ends_on") > F.lit("2023-07-10").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "channel_promo_code_key",
        "promo_id",
        "promotion_promo_code",
        "running_max_promo_id",
        "running_count_promo_id",
        (F.col("running_max_promo_id") * F.col("running_count_promo_id")).alias("running_max_promo_id_times_running_count_promo_id"),
    )
)

remaining_keys = (
    base
    .select("promo_code")
    .intersect(
        df_promotions
        .filter(F.col("promo_code").isin(["VIP", "WELCOME5"]))
        .select("promo_code")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .withColumn("running_count_promo_id_tier", F.when(F.col("running_count_promo_id") >= 18, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "ends_on"))
    .groupingSets(
        [["running_count_promo_id_tier", "quarter_start", "running_count_promo_id"], ["running_count_promo_id_tier", "quarter_start"], ["running_count_promo_id_tier"], []],
        "running_count_promo_id_tier",
        "quarter_start",
        "running_count_promo_id",
    )
    .agg(
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("running_max_promo_id"))).alias("bundle_total_running_max_promo_id"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
        F.count_if(F.col("promotion_promo_code") == "SPRING10").alias("spring10_events_2"),
    )
)

result = (
    summary
    .select(
        "running_count_promo_id",
        "num_promotion_promo_codes",
        "bundle_total_running_max_promo_id",
        "spring10_events",
        "spring10_events_2",
    )
    .orderBy(F.col("running_count_promo_id").desc())
)
