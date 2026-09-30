from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w3 = Window.orderBy(F.col("discount_pct").desc())
w4 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.rank().over(w).alias("discount_pct_rank"),
        F.lead("discount_pct", 1).over(w2).alias("next_discount_pct"),
        F.rank().over(w3).alias("discount_pct_rank_2"),
        F.first("channel", ignorenulls=True).over(w4).alias("first_known_channel"),
    )
)

result = (
    enriched
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "next_discount_pct",
        "discount_pct_rank_2",
        "first_known_channel",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("starts_on").desc())
    .limit(20)
)
