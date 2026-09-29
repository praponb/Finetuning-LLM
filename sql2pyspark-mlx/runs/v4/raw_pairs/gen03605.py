from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-5, Window.currentRow)
w_part = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id")

filtered = (
    df_promotions
    .filter(~F.col("channel").isin(["store", "partner", "web"]))
    .select(
        "*",
        F.length("channel").alias("channel_length"),
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_5"),
        F.lag("discount_pct", 1).over(w_part).alias("prev_discount_pct"),
    )
)
