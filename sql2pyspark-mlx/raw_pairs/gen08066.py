from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-07-27").cast("date")) & (F.col("discount_pct") >= 0.18))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_trunc("YEAR", "starts_on").alias("starts_on_period"),
    )
)

metrics = (
    base
    .select(
        "id",
        "starts_on",
        F.sum("discount_pct").over(w).alias("running_sum_discount_pct"),
        F.count("discount_pct").over(w).alias("running_count_discount_pct"),
        F.avg("discount_pct").over(w).alias("running_avg_discount_pct"),
    )
)

base_promotions = (
    metrics
    .filter(F.col("starts_on") >= F.lit("2023-07-19").cast("date"))
    .select(
        "id",
        "starts_on",
        "running_sum_discount_pct",
        "running_count_discount_pct",
        "running_avg_discount_pct",
        F.when(F.col("running_avg_discount_pct") >= 0.29, "High").when(F.col("running_avg_discount_pct") >= 0.19, "Medium").otherwise("Low").alias("running_avg_discount_pct_tier"),
        F.floor("running_avg_discount_pct").alias("running_avg_discount_pct_whole"),
    )
)

cleaned = (
    base_promotions
    .filter(F.col("starts_on") >= F.lit("2023-08-01").cast("date"))
    .withColumn("running_count_discount_pct_rounded", F.round("running_count_discount_pct", 0))
)

result = (
    cleaned
    .select(
        "id",
        "starts_on",
        "running_sum_discount_pct",
        "running_avg_discount_pct",
        "running_avg_discount_pct_tier",
        "running_avg_discount_pct_whole",
        "running_count_discount_pct_rounded",
    )
    .orderBy(
        F.col("running_count_discount_pct_rounded").asc_nulls_last(),
        F.col("running_avg_discount_pct").desc(),
    )
)
