from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w3 = Window.partitionBy("channel").orderBy(F.col("vip_rows").desc())

metrics = (
    df_promotions
    .withColumn("last3_max_discount_pct", F.max("discount_pct").over(w_hist))
    .withColumn("last3_count_discount_pct", F.count("discount_pct").over(w_hist))
    .withColumn("last3_sum_discount_pct", F.sum("discount_pct").over(w_hist))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1, 0).over(w_ordered))
)

result = (
    metrics
    .groupBy("channel", "promo_code")
    .agg(
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_rows"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.min("last3_max_discount_pct").alias("min_last3_max_discount_pct"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
    )
    .withColumn("vip_rows_dense_rank", F.dense_rank().over(w3))
    .orderBy("num_rows")
)
