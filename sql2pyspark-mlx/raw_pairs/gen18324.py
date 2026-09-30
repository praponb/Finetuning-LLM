from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_promotions
    .filter(F.col("channel").isin(["web", "store"]))
    .select(
        "id",
        "promo_code",
        "ends_on",
        F.ntile(5).over(w).alias("discount_pct_bucket"),
        F.lead("discount_pct", 2, 0).over(w2).alias("next_discount_pct"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_6"),
        F.ntile(3).over(w2).alias("discount_pct_bucket_2"),
    )
)
