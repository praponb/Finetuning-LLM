from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val_whole").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy("val")
w3 = Window.partitionBy(F.month("effective_from"))
w4 = Window.partitionBy("plan")

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

base = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-05-04").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        F.floor("val").alias("val_whole"),
    )
)

scored = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("val_whole_percent_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
    )
)

metrics = (
    scored
    .select(
        "plan",
        "effective_from",
        "currency",
        "val",
        "val_whole",
        "val_whole_percent_rank",
        "val_percent_rank",
        F.stddev_samp("val").over(w3).alias("effective_from_month_stddev_samp_val"),
    )
)

remaining_keys = (
    metrics
    .select("currency")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("currency")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("effective_from_month_stddev_samp_val").between(56, 63))
    .select(
        "plan",
        "effective_from",
        "val_whole",
        "val_whole_percent_rank",
        "effective_from_month_stddev_samp_val",
        F.sum("val_whole").over(w4).alias("plan_sum_val_whole"),
    )
)

base_price_tiers = (
    with_windows
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-02-02"))) & F.col("val_whole_percent_rank").between(0.55, 0.62))
    .withColumn("val_whole_filled", F.coalesce("val_whole", F.lit(0)))
)

result = (
    base_price_tiers
    .select(
        "plan",
        "effective_from",
        "val_whole",
        "val_whole_percent_rank",
        "effective_from_month_stddev_samp_val",
        "plan_sum_val_whole",
        F.when(F.col("plan").rlike("^(enterprise|basic)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").desc_nulls_last())
)
