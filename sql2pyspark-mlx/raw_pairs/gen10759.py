from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on_period", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("ends_on_period", "id", "promo_id")
w4 = Window.partitionBy("channel").orderBy("ends_on_period", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("channel").orderBy("ends_on_period", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("channel", "promo_code").orderBy("session_start_flag", "id", "promo_id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & ~F.col("channel").isin(["store", "partner"]))
    .select(
        "*",
        F.concat("promo_code", F.lit(" - "), "channel").alias("promo_code_channel"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.date_trunc("week", "ends_on").alias("ends_on_period"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("ends_on_period").over(w).isNull() | (F.col("ends_on_period") > F.lag("ends_on_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

attributed = (
    sessionized
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("a.ends_on_period").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_gaps_2 = (
    attributed
    .withColumn("session_start_flag", F.when(F.col("ends_on_period") - F.lag("ends_on_period").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps_2.withColumn("island_id", F.sum("session_start_flag").over(w4))

flagged = (
    islands
    .withColumn("new_group_flag", F.when(F.col("ends_on_period") - F.lag("ends_on_period").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized_promotions = flagged.withColumn("session_no", F.sum("new_group_flag").over(w4))

result = (
    sessionized_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "promo_code_channel",
        "channel_filled",
        "ends_on_period",
        "resolved_channel",
        "is_new_burst",
        "burst_id",
        "promo_id",
        "promotion_discount_pct",
        "new_group_flag",
        "session_no",
        F.count("session_start_flag").over(w_hist).alias("last7_count_session_start_flag"),
        F.max("session_start_flag").over(w_hist).alias("last7_max_session_start_flag"),
        F.sum("session_start_flag").over(w_hist).alias("last7_sum_session_start_flag"),
        F.avg("is_new_burst").over(w6).alias("is_new_burst_avg_2"),
    )
)
