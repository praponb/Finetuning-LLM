from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("discount_pct_tier").orderBy(F.col("spring10_max_discount_pct").desc())

ranked = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("discount_pct") >= 0.29, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        "promo_code",
    )
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct"))).alias("spring10_max_discount_pct"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
    )
    .withColumn("spring10_max_discount_pct_percent_rank", F.percent_rank().over(w2))
)
