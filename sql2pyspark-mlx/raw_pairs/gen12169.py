from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w_part = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location")
w5 = Window.partitionBy("dept_name").orderBy(F.col("last_known_budget_scaled").desc_nulls_first())
w6 = Window.partitionBy("dept_name").orderBy(F.col("last_known_budget_scaled").desc_nulls_last())
w7 = Window.partitionBy("dept_name")
w8 = Window.partitionBy("dept_name").orderBy(F.col("dept_name_min_budget_share_2").desc_nulls_last())

filtered = (
    df_departments
    .filter(F.col("budget") < 3140000)
    .select(
        "*",
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

with_windows = (
    filtered
    .filter(~F.col("dept_name").isin(["Marketing", "Finance"]))
    .select(
        "id",
        "dept_name",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.lead("budget", 1).over(w2).alias("next_budget"),
        F.last("budget", ignorenulls=True).over(w_part).alias("last_known_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share_2"),
    )
)

kept_keys = (
    with_windows
    .select("id", "dept_name")
    .intersectAll(
        df_departments
        .filter(F.col("dept_name") == "Marketing")
        .select("id", "dept_name")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "budget_share",
        "next_budget",
        "budget_share_2",
        F.round((F.col("last_known_budget") * 0.85), 2).alias("last_known_budget_scaled"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_clean"),
    )
)

scored = (
    valid
    .withColumn("last_known_budget_scaled_percent_rank", F.percent_rank().over(w5))
    .withColumn("last_known_budget_scaled_percent_rank_2", F.percent_rank().over(w6))
    .withColumn("dept_name_min_budget_share_2", F.min("budget_share_2").over(w7))
    .withColumn("next_budget_share", F.round((F.col("next_budget") / F.sum("next_budget").over(w7)), 4))
)

result = (
    scored
    .select(
        "id",
        "dept_name",
        "budget_share",
        "last_known_budget_scaled",
        "last_known_budget_scaled_percent_rank",
        "dept_name_min_budget_share_2",
        "next_budget_share",
        F.rank().over(w8).alias("dept_name_min_budget_share_2_rank"),
        F.sum("next_budget").over(w7).alias("dept_name_sum_next_budget"),
        F.stddev_samp("last_known_budget_scaled").over(w7).alias("dept_name_stddev_samp_last_known_budget_scaled"),
    )
)
