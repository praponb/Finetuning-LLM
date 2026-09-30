from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("ends_on").desc(), "id")

ranked = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .withColumn("year_start", F.date_trunc("YEAR", "starts_on"))
    .groupingSets([["channel", "year_start"], ["channel"], []], "channel", "year_start")
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.avg(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct"))).alias("spring10_avg_discount_pct"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.count_if(F.col("promo_code") == "BUNDLE").alias("bundle_events"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("spring10_avg_discount_pct")
)
