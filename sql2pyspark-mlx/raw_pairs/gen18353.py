from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.year("ends_on"), "channel").orderBy("discount_pct")

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_3"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
        F.percent_rank().over(w_grp).alias("discount_pct_percent_rank"),
    )
)
