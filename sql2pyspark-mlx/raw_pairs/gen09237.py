from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy(F.date_trunc("MONTH", "effective_from"), "currency").orderBy(F.col("tier_value").desc_nulls_first())
w_grp = Window.orderBy(F.col("tier_value_dense_rank").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value_dense_rank").desc())
w5 = Window.partitionBy(F.year("effective_from")).orderBy(F.col("tier_value_dense_rank").desc_nulls_last())
w6 = Window.partitionBy("activity_month").orderBy(F.col("cnt").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

enriched = (
    flattened
    .select(
        "id",
        "effective_from",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank_2"),
    )
)

with_windows = (
    enriched
    .withColumn("tier_value_dense_rank_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("tier_value_dense_rank_rank", F.rank().over(w4))
    .withColumn("tier_value_dense_rank_dense_rank_2", F.dense_rank().over(w5))
)

base = (
    with_windows
    .filter(F.col("effective_from") > F.lit("2024-06-15").cast("date"))
    .select(
        "effective_from",
        "tier_value_dense_rank",
        "tier_value_dense_rank_2",
        "tier_value_dense_rank_dense_rank",
        "tier_value_dense_rank_dense_rank_2",
        F.date_trunc("WEEK", "effective_from").alias("effective_from_period"),
        F.col("tier_value_dense_rank_2").cast("double").alias("tier_value_dense_rank_2_num"),
    )
)

cleaned = (
    base
    .filter((F.col("effective_from") >= F.lit("2024-06-01").cast("date")) & (F.col("effective_from") < "2024-10-01"))
    .withColumn("tier_value_dense_rank_rounded", F.round("tier_value_dense_rank", 0))
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("month", "effective_from").alias("activity_month"),
        F.when(F.col("tier_value_dense_rank_2_num") >= 85, "PREMIUM").otherwise("STANDARD").alias("tier_value_dense_rank_2_num_tier"),
    )
    .agg(
        F.max("effective_from_period").alias("last_effective_from_period"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cnt_dense_rank", F.dense_rank().over(w6))
)
