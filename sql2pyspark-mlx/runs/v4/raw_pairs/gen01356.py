from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code", "channel")
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
