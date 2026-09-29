from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("value").orderBy(F.col("qty_times_product_id").desc_nulls_first())
w2 = Window.partitionBy("value")
w3 = Window.partitionBy("value").orderBy(F.col("qty").asc_nulls_first())
w4 = Window.orderBy(F.col("qty_times_product_id").desc_nulls_first())
w5 = Window.orderBy(F.col("qty_times_product_id").desc())
w6 = Window.partitionBy("value").orderBy(F.col("qty_times_product_id_percent_rank").desc())
w7 = Window.partitionBy("value").orderBy(F.col("qty_times_product_id_percent_rank").desc_nulls_first())
w_ordered = Window.partitionBy("value").orderBy(F.col("qty_times_product_id").asc_nulls_last())

lines = df_carts.select("id", "coupon_codes", F.inline("items"))

exploded = lines.select("id", "product_id", "qty", F.explode("coupon_codes").alias("value"))

cleaned = (
    exploded
    .filter(F.col("product_id") == 4)
    .select("*", (F.col("qty") * F.col("product_id")).alias("qty_times_product_id"))
)

filtered = (
    cleaned
    .filter(F.col("value") == "WELCOME5")
    .select(
        "id",
        "qty",
        "value",
        "qty_times_product_id",
        F.trim("value").alias("clean_value"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("qty_times_product_id_dense_rank"),
        F.max("qty").over(w2).alias("value_max_qty"),
        F.cume_dist().over(w3).alias("qty_cume_dist"),
        F.dense_rank().over(w4).alias("qty_times_product_id_dense_rank_2"),
        F.percent_rank().over(w5).alias("qty_times_product_id_percent_rank"),
    )
)

with_windows = (
    scored
    .withColumn("value_avg_qty_times_product_id", F.avg("qty_times_product_id").over(w2))
    .withColumn("qty_times_product_id_percent_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("qty_times_product_id_percent_rank_dense_rank", F.dense_rank().over(w7))
    .withColumn("qty_times_product_id_rank", F.rank().over(w_ordered))
    .withColumn("qty_times_product_id_percent_rank_2", F.percent_rank().over(w5))
)

valid = (
    with_windows
    .filter(F.col("qty_times_product_id_rank") < 8)
    .select(
        "*",
        F.col("qty_times_product_id_dense_rank").cast("double").alias("qty_times_product_id_dense_rank_num"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("qty_times_product_id_percent_rank_2") >= 0.73, "PREMIUM").otherwise("STANDARD").alias("qty_times_product_id_percent_rank_2_tier"),
        F.when(F.col("qty_times_product_id_percent_rank_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("qty_times_product_id_percent_rank_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("qty_times_product_id_percent_rank_cume_dist", 0.9).alias("qty_times_product_id_percent_rank_cume_dist_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("value") == "BUNDLE").alias("bundle_events"),
    )
    .filter(F.col("qty_times_product_id_percent_rank_cume_dist_p90") >= 0)
)
