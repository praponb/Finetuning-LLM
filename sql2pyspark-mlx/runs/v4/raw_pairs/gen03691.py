from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("QUARTER", "starts_on").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.count_if(F.col("channel") == "partner").alias("partner_events_2"),
    )
    .orderBy("partner_events_2")
)
