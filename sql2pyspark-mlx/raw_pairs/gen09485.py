from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_rounded").desc_nulls_last(), "id")
w3 = Window.partitionBy("channel").orderBy("rank_in_group", "id")

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-07-01")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked_promotions = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked_promotions
    .withColumn("rank_in_group_2", F.row_number().over(w3))
    .filter(F.col("rank_in_group_2") == 1)
    .select("id", "starts_on", "discount_pct_rounded", "resolved_channel", "row_num")
)

result = (
    latest
    .withColumn("discount_pct_rounded_tier", F.when(F.col("discount_pct_rounded") >= 70, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("YEAR", "starts_on"))
    .groupingSets(
        [["discount_pct_rounded_tier", "year_start"], ["discount_pct_rounded_tier"], []],
        "discount_pct_rounded_tier",
        "year_start",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("discount_pct_rounded", [0.25, 0.5, 0.75]).alias("discount_pct_rounded_quartiles"),
        F.round(F.avg("discount_pct_rounded"), 2).alias("avg_discount_pct_rounded"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("cnt") > 1)
)
