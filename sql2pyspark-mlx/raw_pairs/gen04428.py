from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w4 = Window.partitionBy(F.month("ends_on"), "promo_code").orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)

with_windows = (
    df_promotions
    .filter(F.month("starts_on").isin([3, 4, 10]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        F.max("discount_pct").over(w).alias("promo_code_max_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_5_prior"),
        F.ntile(10).over(w3).alias("discount_pct_bucket"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_5"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "promo_code_max_discount_pct",
        "discount_pct_avg_5_prior",
        "discount_pct_bucket",
        "discount_pct_avg_5",
    )
)
