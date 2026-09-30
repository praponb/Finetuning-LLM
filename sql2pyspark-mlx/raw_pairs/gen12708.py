from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.count("discount_pct").over(w).alias("last4_count_discount_pct"),
        F.avg("discount_pct").over(w).alias("last4_avg_discount_pct"),
        F.max("discount_pct").over(w).alias("last4_max_discount_pct"),
        F.sum("discount_pct").over(w).alias("last4_sum_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_6"),
    )
)
