from pyspark.sql import functions as F

result = (
    df_promotions
    .cube(F.date_trunc("WEEK", "starts_on").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_pct_per_store"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
)
