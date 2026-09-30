from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")
w3 = Window.partitionBy("channel")

enriched = (
    df_promotions
    .withColumn("promo_code_sum_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("discount_pct_share_2", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("discount_pct_share") >= 0.64, "PREMIUM").otherwise("STANDARD").alias("discount_pct_share_tier"),
        "promo_code",
    )
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.count_if(F.col("promo_code") == "WELCOME5").alias("welcome5_events"),
    )
    .filter(F.col("partner_events") >= 1)
)
