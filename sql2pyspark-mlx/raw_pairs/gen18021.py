from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_promotions
    .filter(F.month("ends_on").isin([6, 8, 10]))
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.count("discount_pct").over(w).alias("promo_code_count_discount_pct"),
        F.ntile(4).over(w2).alias("discount_pct_bucket"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_3_prior"),
        F.last("channel", ignorenulls=True).over(w4).alias("last_known_channel"),
    )
)

result = (
    scored
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "discount_pct_bucket",
        "last_known_channel",
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("discount_pct").desc())
    .limit(20)
)
