from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("discount_pct").alias("avg_discount_pct"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("promo_code").desc_nulls_first())
)
