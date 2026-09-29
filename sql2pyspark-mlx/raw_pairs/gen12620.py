from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w_ordered = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last())
w_recent = Window.partitionBy("dept_name")
w4 = Window.partitionBy("dept_name").orderBy(F.col("location_stddev_samp_budget").desc_nulls_first())

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("clean_location", F.trim("location"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("UNKNOWN")))
)

remaining_keys = (
    cleaned
    .select("id")
    .intersect(
        df_departments
        .filter(F.col("budget").between(2259000, 3224000))
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "dept_name",
        "budget",
        "clean_location",
        "resolved_location",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.cume_dist().over(w_ordered).alias("budget_cume_dist"),
        F.stddev_samp("budget").over(w).alias("location_stddev_samp_budget"),
    )
)

scored = (
    metrics
    .filter(F.col("budget") < 2778000)
    .select(
        "*",
        F.sum("location_stddev_samp_budget").over(w_recent).alias("dept_name_sum_location_stddev_samp_budget"),
        F.rank().over(w_ordered).alias("budget_rank"),
        F.dense_rank().over(w4).alias("location_stddev_samp_budget_dense_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") >= 1443000))
    .select(
        "dept_name",
        "budget",
        "clean_location",
        "budget_share",
        "budget_cume_dist",
        "location_stddev_samp_budget",
        "budget_rank",
        F.when(F.col("dept_name_sum_location_stddev_samp_budget") >= 18894154, "High").when(F.col("dept_name_sum_location_stddev_samp_budget") >= 9809408, "Medium").otherwise("Low").alias("dept_name_sum_location_stddev_samp_budget_tier"),
        F.when(F.col("budget_rank") >= 7, "High").when(F.col("budget_rank") >= 3, "Medium").otherwise("Low").alias("budget_rank_band"),
    )
)

result = (
    filtered
    .groupBy("dept_name")
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .orderBy(F.col("row_count").desc())
)
