from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w_part = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy(F.year("effective_from"), "currency").orderBy("val")
w5 = Window.orderBy(F.col("val").asc_nulls_last())
w6 = Window.partitionBy(F.date_trunc("WEEK", "effective_from")).orderBy(F.col("plan_max_val").desc_nulls_first())
w7 = Window.partitionBy(F.year("effective_from"), "currency")
w8 = Window.partitionBy("plan").orderBy(F.col("val_dense_rank").desc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

scored = (
    flattened
    .filter(F.col("effective_from").between("2023-11-01", "2025-06-13"))
    .select(
        "*",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.rank().over(w_part).alias("val_rank"),
        F.max("val").over(w3).alias("plan_max_val"),
        F.rank().over(w4).alias("val_rank_2"),
        F.cume_dist().over(w5).alias("val_cume_dist"),
    )
)

scored_price_tiers = (
    scored
    .withColumn("plan_max_val_cume_dist", F.cume_dist().over(w6))
    .withColumn("val_rank_share", F.round((F.col("val_rank") / F.sum("val_rank").over(w7)), 4))
    .withColumn("val_dense_rank_dense_rank", F.dense_rank().over(w8))
)

cleaned = (
    scored_price_tiers
    .filter((F.col("effective_from") > F.lit("2024-02-14").cast("date")) & (F.col("val_cume_dist") < 0.4))
    .select("*", F.when(F.col("plan") == "starter", 1).otherwise(0).alias("is_starter"))
)

base = (
    cleaned
    .filter(F.col("effective_from") > F.lit("2024-02-08").cast("date"))
    .withColumn("plan_max_val_plus_val_rank", (F.col("plan_max_val") + F.col("val_rank")))
    .withColumn("is_starter_rounded", F.round("is_starter", 0))
)

valid = (
    base
    .filter(F.col("effective_from") > F.lit("2024-02-20").cast("date"))
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "val_dense_rank",
        "val_rank",
        "val_rank_2",
        "val_cume_dist",
        "plan_max_val_cume_dist",
        "val_rank_share",
        "val_dense_rank_dense_rank",
        "plan_max_val_plus_val_rank",
        "is_starter_rounded",
        F.when(F.col("plan_max_val") >= 69, "High").when(F.col("plan_max_val") >= 35, "Medium").otherwise("Low").alias("plan_max_val_tier"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum("val_dense_rank") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("val_dense_rank_per_usd"),
        F.sum(F.when(F.col("currency") == "THB", F.col("val_rank_share")).otherwise(0)).alias("thb_val_rank_share"),
    )
    .filter(F.col("thb_val_rank_share") > 0)
    .orderBy("week_start")
)
