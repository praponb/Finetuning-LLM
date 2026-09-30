from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy(F.month("ends_on"), "promo_code")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w_hist = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w5 = Window.orderBy("discount_pct")

result = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.min("discount_pct").over(w_grp).alias("ends_on_month_min_discount_pct"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
        F.ntile(10).over(w3).alias("discount_pct_bucket"),
        F.cume_dist().over(w_hist).alias("discount_pct_cume_dist"),
        F.rank().over(w5).alias("discount_pct_rank"),
    )
)
