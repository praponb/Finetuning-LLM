from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("promo_code_length").desc())
w3 = Window.orderBy(F.col("promo_code_length").desc(), "id")
w4 = Window.orderBy(F.col("promo_code_length").asc_nulls_last())
w5 = Window.partitionBy("promo_code").orderBy("promo_code_length")
w6 = Window.partitionBy("promo_code").orderBy(F.col("promo_code_length_dense_rank").asc_nulls_last(), "id")
w7 = Window.partitionBy("promo_code").orderBy(F.col("spring10_rows").desc())

latest = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "promo_code")
)

base = (
    latest
    .filter(F.col("promo_code").isin(["VIP", "SPRING10", "BUNDLE"]))
    .select(
        "*",
        F.lower("promo_code").alias("promo_code_lower"),
        F.length("promo_code").alias("promo_code_length"),
    )
)

with_windows = (
    base
    .filter(F.col("promo_code_length").between(37, 52))
    .withColumn("promo_code_length_cume_dist", F.cume_dist().over(w2))
    .withColumn("promo_code_length_bucket", F.ntile(10).over(w3))
    .withColumn("promo_code_length_dense_rank", F.dense_rank().over(w4))
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

deduped_promotions = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped_promotions
    .groupBy(
        "promo_code",
        F.when(F.col("promo_code_length_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("promo_code_length_dense_rank_tier"),
    )
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.avg(F.when(F.col("promo_code") == "WELCOME5", F.col("promo_code_length_dense_rank"))).alias("welcome5_avg_promo_code_length_dense_rank"),
        F.round(F.avg("promo_code_length_bucket"), 2).alias("avg_promo_code_length_bucket"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .withColumn("spring10_rows_percent_rank", F.percent_rank().over(w7))
    .orderBy("spring10_rows")
)
