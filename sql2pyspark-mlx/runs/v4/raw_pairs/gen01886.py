from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val_int").desc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("val_int").desc_nulls_first())
w5 = Window.orderBy(F.col("val_rank").desc())
w_part = Window.orderBy("val_int_rank")
w7 = Window.partitionBy("plan").orderBy(F.col("val_rank_rank").desc_nulls_first())
w8 = Window.orderBy(F.col("plan_count_val").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

cleaned = (
    exploded
    .filter(F.col("effective_from") >= F.lit("2024-09-22").cast("date"))
    .select("plan", "effective_from", "val", F.trim("plan").alias("clean_plan"))
)

valid = (
    cleaned
    .filter(F.col("effective_from") > F.lit("2024-06-06").cast("date"))
    .select(
        "*",
        F.col("val").cast("int").alias("val_int"),
        F.when(F.col("val") >= 67, "High").when(F.col("val") >= 31, "Medium").otherwise("Low").alias("val_band"),
    )
)

scored = (
    valid
    .withColumn("val_int_percent_rank", F.percent_rank().over(w))
    .withColumn("plan_count_val", F.count("val").over(w2))
    .withColumn("val_rank", F.rank().over(w3))
)

with_windows = (
    scored
    .withColumn("val_int_rank", F.rank().over(w4))
    .withColumn("plan_stddev_samp_val_int", F.stddev_samp("val_int").over(w2))
    .withColumn("plan_sum_val_int", F.sum("val_int").over(w2))
)

enriched = (
    with_windows
    .withColumn("val_rank_rank", F.rank().over(w5))
    .withColumn("plan_min_val_int_rank", F.min("val_int_rank").over(w2))
    .withColumn("val_int_rank_share", F.round((F.col("val_int_rank") / F.sum("val_int_rank").over(w2)), 4))
    .withColumn("plan_count_val_int", F.count("val_int").over(w2))
)

enriched_price_tiers = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w_part).alias("val_int_rank_dense_rank"),
        F.rank().over(w7).alias("val_rank_rank_rank"),
        F.round((F.col("plan_min_val_int_rank") / F.sum("plan_min_val_int_rank").over(w2)), 4).alias("plan_min_val_int_rank_share"),
        F.dense_rank().over(w8).alias("plan_count_val_dense_rank"),
    )
)

result = (
    enriched_price_tiers
    .select(
        "plan",
        "effective_from",
        "val",
        "val_band",
        "val_int_percent_rank",
        "plan_count_val",
        "val_int_rank",
        "plan_sum_val_int",
        "val_rank_rank",
        F.when(F.col("plan").rlike("^(pro|basic)$"), "CORE").when(F.col("plan").rlike("^(starter|enterprise)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("plan_zone"),
    )
    .orderBy(F.col("val_int_rank").desc_nulls_first())
)
