from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_promotions
    .select(
        "*",
        F.sum("discount_pct").over(w_hist).alias("last7_sum_discount_pct"),
        F.max("discount_pct").over(w_hist).alias("last7_max_discount_pct"),
        F.min("discount_pct").over(w_hist).alias("last7_min_discount_pct"),
        F.count("discount_pct").over(w_hist).alias("last7_count_discount_pct"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "last7_max_discount_pct",
        "last7_min_discount_pct",
        "last7_count_discount_pct",
        F.when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_B").when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").asc_nulls_first(), "last7_count_discount_pct")
)
