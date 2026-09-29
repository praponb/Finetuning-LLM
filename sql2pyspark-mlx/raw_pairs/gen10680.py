from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code")

with_windows = (
    df_promotions
    .select(
        "*",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
        F.stddev_samp("discount_pct").over(w3).alias("promo_code_stddev_samp_discount_pct"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("MONTH", "ends_on").alias("activity_month"),
        F.when(F.col("cumulative_discount_pct") >= 1, "LARGE").otherwise("SMALL").alias("cumulative_discount_pct_tier"),
    )
    .agg(
        F.sum("cumulative_discount_pct").alias("total_cumulative_discount_pct"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list_2"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_rows"),
    )
    .filter(F.col("total_cumulative_discount_pct") > 0)
)
