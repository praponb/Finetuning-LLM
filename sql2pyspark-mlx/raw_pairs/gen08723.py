from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-2, -1)

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_2_prior"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_2_prior_2"),
    )
)
