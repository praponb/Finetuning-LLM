from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", "coupon_codes", F.col("elem.promo"))
)

result = (
    exploded
    .groupBy("promo")
    .agg(
        F.count_if(F.col("promo") == "VIP").alias("vip_events"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count("*").alias("cnt"),
        F.countDistinct("promo").alias("distinct_promos"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("welcome5_count").desc_nulls_first())
)
