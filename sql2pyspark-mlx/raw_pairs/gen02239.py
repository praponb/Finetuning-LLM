from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy(F.month("starts_on"), "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w_running = Window.orderBy("seq_no_scaled", "id")
w_hist = Window.orderBy("weeks_ends_on_to_starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last_known_weeks_ends_on_to_starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("next_weeks_ends_on_to_starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_promotions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "promo_code", "starts_on", "ends_on")
)

base = (
    ranked
    .filter(F.col("ends_on") <= "2025-05-01")
    .withColumn("weeks_ends_on_to_starts_on", (F.datediff("starts_on", "ends_on") / 7))
)

enriched = (
    base
    .filter(F.col("promo_code").isin(["WELCOME5", "BUNDLE", "VIP"]))
    .withColumn("next_weeks_ends_on_to_starts_on", F.lead("weeks_ends_on_to_starts_on").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("last_known_weeks_ends_on_to_starts_on", F.last("weeks_ends_on_to_starts_on", ignorenulls=True).over(w3))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "weeks_ends_on_to_starts_on",
        "next_weeks_ends_on_to_starts_on",
        "seq_no",
        "last_known_weeks_ends_on_to_starts_on",
    )
)

filtered = (
    latest
    .filter(F.col("weeks_ends_on_to_starts_on") >= 20)
    .select(
        "id",
        "weeks_ends_on_to_starts_on",
        "next_weeks_ends_on_to_starts_on",
        "seq_no",
        "last_known_weeks_ends_on_to_starts_on",
        F.round((F.col("seq_no") * 1.07), 2).alias("seq_no_scaled"),
    )
)

enriched_promotions = (
    filtered
    .withColumn("seq_no_scaled_bucket", F.ntile(3).over(w_running))
    .withColumn("cumulative_weeks_ends_on_to_starts_on", F.sum("weeks_ends_on_to_starts_on").over(w_hist))
    .withColumn("first_known_last_known_weeks_ends_on_to_starts_on", F.first("last_known_weeks_ends_on_to_starts_on", ignorenulls=True).over(w_hist))
    .withColumn("cumulative_next_weeks_ends_on_to_starts_on", F.sum("next_weeks_ends_on_to_starts_on").over(w7))
    .withColumn("running_weeks_ends_on_to_starts_on", F.sum("weeks_ends_on_to_starts_on").over(w8))
)

result = (
    enriched_promotions
    .groupBy(
        "seq_no",
        F.when(F.col("cumulative_next_weeks_ends_on_to_starts_on") >= 397, "HIGH").otherwise("LOW").alias("cumulative_next_weeks_ends_on_to_starts_on_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("cumulative_next_weeks_ends_on_to_starts_on", [0.25, 0.5, 0.75]).alias("cumulative_next_weeks_ends_on_to_starts_on_quartiles"),
        F.round(F.avg("seq_no_scaled"), 2).alias("avg_seq_no_scaled"),
    )
)
