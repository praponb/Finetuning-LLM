from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tag_length").asc_nulls_first())
w2 = Window.partitionBy("tag").orderBy(F.col("tag_length").desc_nulls_first())
w3 = Window.partitionBy("tag")
w4 = Window.partitionBy("tag").orderBy(F.col("tag_avg_tag_length").asc_nulls_first())
w5 = Window.partitionBy("tag").orderBy(F.col("tag_avg_tag_length").asc_nulls_last())
w6 = Window.orderBy(F.col("tag_avg_tag_length_percent_rank").asc_nulls_first())

lines = df_carts.select("id", F.explode("coupon_codes").alias("tag"))

remaining_keys = (
    lines
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("created_at") < "2025-08-01")
        .select("id")
    )
)

verified = (
    lines
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("tag").isin(["WELCOME5", "BUNDLE", "VIP"]))
    .select("*", F.trim("tag").alias("clean_tag"), F.length("tag").alias("tag_length"))
)

enriched = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("tag_length_dense_rank"),
        F.dense_rank().over(w2).alias("tag_length_dense_rank_2"),
        F.avg("tag_length").over(w3).alias("tag_avg_tag_length"),
    )
)

enriched_carts = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w4).alias("tag_avg_tag_length_percent_rank"),
        F.dense_rank().over(w5).alias("tag_avg_tag_length_dense_rank"),
    )
)

enriched_carts_2 = (
    enriched_carts
    .withColumn("tag_avg_tag_length_percent_rank_rank", F.rank().over(w6))
    .withColumn("tag_sum_tag_length_dense_rank", F.sum("tag_length_dense_rank").over(w3))
)

result = (
    enriched_carts_2
    .groupBy(
        F.when(F.col("tag_avg_tag_length_percent_rank") >= 0.75, "LARGE").otherwise("SMALL").alias("tag_avg_tag_length_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("tag_avg_tag_length_percent_rank"), 2).alias("avg_tag_avg_tag_length_percent_rank"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.sum(F.when(F.col("tag") == "VIP", F.col("tag_avg_tag_length")).otherwise(0)).alias("vip_tag_avg_tag_length"),
    )
    .orderBy(F.col("vip_tag_avg_tag_length").desc_nulls_last())
)
