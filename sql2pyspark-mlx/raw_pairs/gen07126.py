from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("discount_pct", "id")
w_hist = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w_grp = Window.partitionBy("channel")
w4 = Window.partitionBy("promo_code")

result = (
    df_promotions
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.ntile(10).over(w_part).alias("discount_pct_bucket"),
        F.ntile(5).over(w_hist).alias("discount_pct_bucket_2"),
        F.min("discount_pct").over(w_grp).alias("channel_min_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share"),
    )
)
