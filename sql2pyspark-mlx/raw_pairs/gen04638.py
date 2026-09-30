from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "ends_on"), "promo_code").orderBy("starts_on", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")
w4 = Window.partitionBy("promo_code").orderBy("discount_pct")

result = (
    df_promotions
    .filter(F.col("ends_on") > "2025-02-01")
    .select(
        "id",
        "channel",
        "discount_pct",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_3_prior"),
        F.lead("discount_pct", 2).over(w2).alias("next_discount_pct"),
        F.row_number().over(w3).alias("row_seq"),
        F.percent_rank().over(w4).alias("discount_pct_percent_rank"),
    )
)
