from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w_hist = Window.partitionBy("channel")

scored = (
    df_promotions
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w_part))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 2).over(w2))
    .withColumn("channel_avg_discount_pct", F.avg("discount_pct").over(w_hist))
)

result = (
    scored
    .groupBy("promo_code")
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("cumulative_discount_pct")).otherwise(0)).alias("vip_cumulative_discount_pct"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
    )
    .filter(F.col("avg_discount_pct") >= 0)
    .orderBy(F.col("spring10_rows").desc_nulls_first())
)
