from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())

with_windows = (
    df_promotions
    .filter(F.col("promo_code").isin(["SPRING10", "BUNDLE", "WELCOME5"]))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
        F.percent_rank().over(w3).alias("discount_pct_percent_rank"),
        F.dense_rank().over(w4).alias("discount_pct_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "promo_code",
        "ends_on",
        "first_known_channel",
        "discount_pct_percent_rank",
    )
    .orderBy(F.col("promo_code").desc_nulls_first())
)
