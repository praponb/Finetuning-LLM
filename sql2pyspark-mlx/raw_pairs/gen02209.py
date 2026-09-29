from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc())
w3 = Window.partitionBy("channel").orderBy("ends_on", "id")
w_part = Window.partitionBy("promo_code").orderBy("discount_pct", "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_promotions = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "channel", "promo_code", "ends_on", "discount_pct", "channel_filled")
)

latest = (
    deduped_promotions
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    latest
    .filter(F.col("channel") != "store")
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "channel_filled",
        F.lag("discount_pct").over(w_part).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w_part).alias("next_discount_pct"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

totals = (
    enriched
    .groupBy(
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
        F.date_trunc("QUARTER", "ends_on").alias("quarter_start"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("next_discount_pct")).otherwise(0)).alias("web_next_discount_pct"),
        F.percentile_approx("seq_no", 0.9).alias("seq_no_p90"),
    )
)

result = (
    totals
    .select("year_start", "quarter_start", "web_next_discount_pct")
    .orderBy(F.col("web_next_discount_pct").asc_nulls_last(), "year_start")
)
