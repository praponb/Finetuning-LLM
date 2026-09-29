from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w2 = Window.orderBy("ends_on", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("channel")
w_grp = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-2, -1)

base = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-07-18").cast("date")) & F.col("channel").isNotNull())
    .withColumn("promo_code_lower", F.lower("promo_code"))
)

result = (
    base
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w))
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w2))
    .withColumn("promo_code_max_discount_pct", F.max("discount_pct").over(w3))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4))
    .withColumn("discount_pct_avg_2_prior", F.avg("discount_pct").over(w_grp))
)
