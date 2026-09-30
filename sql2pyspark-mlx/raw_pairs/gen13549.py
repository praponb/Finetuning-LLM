from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("starts_on"), "promo_code").orderBy("ends_on", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc())

metrics = (
    df_promotions
    .select(
        "*",
        F.sum("discount_pct").over(w).alias("running_discount_pct"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct_2"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_5"),
        F.percent_rank().over(w4).alias("discount_pct_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "running_discount_pct",
        "discount_pct_avg_5",
        "discount_pct_percent_rank",
    )
    .orderBy(
        F.col("discount_pct_percent_rank").desc_nulls_last(),
        "running_discount_pct",
    )
)
