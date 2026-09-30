from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy(F.date_trunc("YEAR", "starts_on"), "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("discount_pct")
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

result = (
    df_promotions
    .filter(F.col("channel").isNull())
    .select(
        "id",
        "starts_on",
        "discount_pct",
        F.ntile(5).over(w).alias("discount_pct_bucket"),
        F.lag("discount_pct", 1).over(w2).alias("prev_discount_pct"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
        F.percent_rank().over(w4).alias("discount_pct_percent_rank"),
        F.ntile(5).over(w5).alias("discount_pct_bucket_2"),
    )
)
