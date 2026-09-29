from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_promotions
    .filter(F.month("starts_on").isin([3, 4, 10]))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.min("discount_pct").over(w_grp).alias("running_min_discount_pct"),
        F.max("discount_pct").over(w_grp).alias("running_max_discount_pct"),
        F.sum("discount_pct").over(w_grp).alias("running_sum_discount_pct"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
    )
)

result = (
    enriched
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "running_max_discount_pct",
        "running_sum_discount_pct",
        "running_discount_pct",
    )
    .orderBy(
        F.col("discount_pct").desc_nulls_last(),
        F.col("running_discount_pct").desc(),
    )
)
