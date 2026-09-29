from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_welcome5").desc_nulls_first())
w2 = Window.orderBy(F.col("is_welcome5").asc_nulls_first())
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("code")
w5 = Window.orderBy(F.col("is_welcome5").desc())

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    flattened
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("is_welcome5_dense_rank"),
        F.rank().over(w2).alias("is_welcome5_rank"),
        F.max("is_welcome5").over(w3).alias("customer_id_max_is_welcome5"),
        F.avg("is_welcome5").over(w4).alias("code_avg_is_welcome5"),
        F.sum("is_welcome5").over(w3).alias("customer_id_sum_is_welcome5"),
    )
)

metrics_carts = (
    metrics
    .withColumn("code_sum_is_welcome5", F.sum("is_welcome5").over(w4))
    .withColumn("is_welcome5_rank_share", F.round((F.col("is_welcome5_rank") / F.sum("is_welcome5_rank").over(w3)), 4))
    .withColumn("is_welcome5_dense_rank_2", F.dense_rank().over(w5))
)

daily = (
    metrics_carts
    .cube("code")
    .agg(
        F.round(F.sum("customer_id_sum_is_welcome5") / F.nullif(F.count(F.when(F.col("code") == "SPRING10", 1)), F.lit(0)), 2).alias("customer_id_sum_is_welcome5_per_spring10"),
        F.percentile_approx("code_sum_is_welcome5", 0.5).alias("code_sum_is_welcome5_p50"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    daily
    .select(
        "code",
        "customer_id_sum_is_welcome5_per_spring10",
        "code_sum_is_welcome5_p50",
        F.when(F.col("code").rlike("^(VIP|WELCOME5)$"), "CORE").when(F.col("code").rlike("^(SPRING10|BUNDLE)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("code_zone"),
    )
    .orderBy(
        F.col("code_sum_is_welcome5_p50").desc(),
        "customer_id_sum_is_welcome5_per_spring10",
    )
)
