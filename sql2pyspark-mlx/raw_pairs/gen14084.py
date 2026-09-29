from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("channel")
w_hist = Window.partitionBy("channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id")

with_windows = (
    df_promotions
    .withColumn("discount_pct_avg_6_prior", F.avg("discount_pct").over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
    .withColumn("discount_pct_bucket", F.ntile(4).over(w_hist))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w4))
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_avg_6_prior",
        "discount_pct_bucket",
        "prev_discount_pct",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
