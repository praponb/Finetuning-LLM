from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code")
w2 = Window.partitionBy("promo_code", "channel")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "*",
        F.sum("discount_pct").over(w).alias("channel_sum_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.sum("discount_pct").over(w3).alias("cumulative_discount_pct"),
    )
)
