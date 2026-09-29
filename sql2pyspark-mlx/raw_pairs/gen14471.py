from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy(F.year("ends_on"), "promo_code").orderBy("discount_pct", "id")

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        F.count("discount_pct").over(w_hist).alias("channel_count_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.ntile(5).over(w3).alias("discount_pct_bucket"),
    )
)
