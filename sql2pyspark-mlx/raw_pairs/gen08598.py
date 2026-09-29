from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "starts_on"), "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy(F.month("ends_on"), "channel").orderBy(F.col("discount_pct").desc(), "id")

result = (
    df_promotions
    .select(
        "id",
        "ends_on",
        "discount_pct",
        F.ntile(10).over(w).alias("discount_pct_bucket"),
        F.ntile(5).over(w2).alias("discount_pct_bucket_2"),
    )
)
