from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct_rounded_rounded").asc_nulls_first())
w2 = Window.partitionBy(F.year("ends_on"), "promo_code").orderBy(F.col("discount_pct_rounded").desc_nulls_first())
w_part = Window.partitionBy("promo_code").orderBy(F.col("ends_on_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)

kept_keys = (
    df_promotions
    .select("channel", "id")
    .exceptAll(
        df_promotions
        .filter(F.col("starts_on") < "2024-11-01")
        .select("channel", "id")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows
    .select("channel")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on") < "2025-03-01")
        .select("channel")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-08-05")))
    .withColumn("discount_pct_rounded", F.round("discount_pct", 0))
    .withColumn("ends_on_period", F.date_trunc("WEEK", "ends_on"))
)

filtered_promotions = (
    filtered
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-09-09"))) & F.month("ends_on").isin([1, 2, 3]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct_rounded",
        "ends_on_period",
        F.round("discount_pct_rounded", 1).alias("discount_pct_rounded_rounded"),
    )
)

enriched = (
    filtered_promotions
    .select(
        "id",
        "channel",
        "ends_on_period",
        "discount_pct_rounded_rounded",
        F.cume_dist().over(w).alias("discount_pct_rounded_rounded_cume_dist"),
        F.cume_dist().over(w2).alias("discount_pct_rounded_cume_dist"),
        F.sum("discount_pct_rounded").over(w_part).alias("discount_pct_rounded_sum_7d"),
    )
)

result = (
    enriched
    .rollup("channel", F.date_trunc("YEAR", "ends_on_period").alias("year_start"))
    .agg(
        F.min("ends_on_period").alias("first_seen"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("num_channels") > 1)
)
