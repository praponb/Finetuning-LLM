from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel", "promo_code")

with_windows = (
    df_promotions
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_share",
    )
    .orderBy(F.col("channel").desc())
)
