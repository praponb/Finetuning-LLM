from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("cumulative_discount_pct").asc_nulls_last())
w4 = Window.partitionBy("channel").orderBy(F.col("cumulative_discount_pct").asc_nulls_first())
w5 = Window.partitionBy("channel").orderBy("ends_on", "id")

enriched = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.sum("discount_pct").over(w_ordered).alias("cumulative_discount_pct"),
        F.first("channel", ignorenulls=True).over(w_recent).alias("first_known_channel"),
    )
)

enriched_promotions = (
    enriched
    .withColumn("cumulative_discount_pct_dense_rank", F.dense_rank().over(w3))
    .withColumn("cumulative_discount_pct_dense_rank_2", F.dense_rank().over(w4))
)

ranked = (
    enriched_promotions
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

matched = (
    ranked
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

valid = (
    matched
    .filter(F.col("promo_code").isNotNull() & (F.col("discount_pct") >= 0.12))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "cumulative_discount_pct",
        "first_known_channel",
        "cumulative_discount_pct_dense_rank",
        "cumulative_discount_pct_dense_rank_2",
        "dedup_rank",
        "promo_id",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
    )
)

result = (
    valid
    .groupBy("channel", F.date_trunc("WEEK", "starts_on").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("partner_events") >= 1)
    .orderBy(F.col("channel").asc_nulls_last())
)
