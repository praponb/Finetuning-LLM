from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct") <= 0.25))
    .select(
        "*",
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct_rounded",
        "resolved_channel",
        F.avg("discount_pct_rounded").over(w).alias("discount_pct_rounded_avg_5"),
        F.avg("discount_pct_rounded").over(w2).alias("discount_pct_rounded_avg_3"),
        F.last("channel", ignorenulls=True).over(w_hist).alias("last_known_channel"),
    )
)
