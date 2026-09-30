from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w4 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")
w5 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())

enriched = (
    df_promotions
    .filter(F.year("starts_on") >= 2025)
    .select(
        "id",
        "starts_on",
        "ends_on",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.avg("discount_pct").over(w_part).alias("discount_pct_avg_5"),
        F.lead("discount_pct", 2).over(w3).alias("next_discount_pct"),
        F.lead("discount_pct", 1).over(w4).alias("next_discount_pct_2"),
        F.rank().over(w5).alias("discount_pct_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ends_on",
        "last_known_channel",
        "discount_pct_avg_5",
        "next_discount_pct",
        "next_discount_pct_2",
        "discount_pct_rank",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
