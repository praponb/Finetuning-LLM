from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")
w2 = Window.orderBy("val")
w3 = Window.orderBy(F.col("plan_min_val").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_min_val").desc_nulls_last())
w_part_2 = Window.partitionBy("plan").orderBy(F.col("plan_min_val").asc_nulls_last())
w6 = Window.orderBy(F.col("plan_min_val_dense_rank").desc_nulls_first())
w7 = Window.orderBy(F.col("val").desc())
w8 = Window.partitionBy("plan").orderBy(F.col("plan_min_val").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

with_windows = (
    exploded
    .select(
        "*",
        F.max("val").over(w_part).alias("plan_max_val"),
        F.min("val").over(w_part).alias("plan_min_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("effective_from") > F.lit("2024-05-09").cast("date"))
    .select(
        "id",
        "plan",
        "val",
        "plan_min_val",
        F.trim("plan").alias("clean_plan"),
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

matched_keys = (
    valid
    .select("plan", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan").isin(["pro", "enterprise"]))
        .select("plan", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_price_tiers = (
    verified
    .select(
        "*",
        F.dense_rank().over(w3).alias("plan_min_val_dense_rank"),
        F.rank().over(w4).alias("plan_min_val_rank"),
        F.cume_dist().over(w_part_2).alias("plan_min_val_cume_dist"),
    )
)

result = (
    with_windows_price_tiers
    .withColumn("plan_min_val_dense_rank_rank", F.rank().over(w6))
    .withColumn("plan_min_val_cume_dist_2", F.cume_dist().over(w_part_2))
    .withColumn("plan_min_val_dense_rank_share", F.round((F.col("plan_min_val_dense_rank") / F.sum("plan_min_val_dense_rank").over(w_part)), 4))
    .withColumn("val_cume_dist", F.cume_dist().over(w7))
    .withColumn("plan_min_val_rank_2", F.rank().over(w8))
)
