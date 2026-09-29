from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("year_start").orderBy(F.col("avg_discount_pct").desc())

latest = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("year", "ends_on").alias("year_start"), "channel")
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_per_partner"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_pct_per_web"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .withColumn("avg_discount_pct_rank", F.rank().over(w2))
)
