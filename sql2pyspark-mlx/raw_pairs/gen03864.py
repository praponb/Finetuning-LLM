from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("ends_on"), "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")

result = (
    df_promotions
    .select(
        "id",
        "ends_on",
        F.ntile(4).over(w).alias("discount_pct_bucket"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
        F.lead("discount_pct", 1).over(w3).alias("next_discount_pct"),
    )
)
