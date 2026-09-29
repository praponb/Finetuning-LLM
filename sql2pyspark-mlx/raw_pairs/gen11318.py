from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w_part = Window.orderBy(F.col("greatest_budget_scaled_budget").asc_nulls_last())
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget_scaled").desc_nulls_first())
w4 = Window.partitionBy("dept_name").orderBy(F.col("greatest_budget_scaled_budget_dense_rank").desc_nulls_first())
w5 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())
w6 = Window.orderBy("greatest_budget_scaled_budget")

filtered = (
    df_departments
    .filter(F.col("budget").between(2581000, 3766000))
    .withColumn("budget_scaled", F.round((F.col("budget") * 1.1), 2))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

base = (
    filtered
    .filter((~F.col("dept_name").eqNullSafe(F.lit("Finance"))) & F.col("budget").between(2270000, 2550000))
    .select(
        "id",
        "dept_name",
        "budget",
        "budget_scaled",
        "dept_name_clean",
        F.greatest("budget_scaled", "budget").alias("greatest_budget_scaled_budget"),
    )
)

matched_keys = (
    base
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Engineering", "Sales", "Legal"]))
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("budget") >= 1888000)
    .select(
        "dept_name",
        "budget",
        "greatest_budget_scaled_budget",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.dense_rank().over(w_part).alias("greatest_budget_scaled_budget_dense_rank"),
        F.sum("greatest_budget_scaled_budget").over(w).alias("dept_name_sum_greatest_budget_scaled_budget"),
        F.rank().over(w3).alias("budget_scaled_rank"),
        F.min("budget").over(w).alias("dept_name_min_budget"),
    )
)

metrics_departments = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w4).alias("greatest_budget_scaled_budget_dense_rank_percent_rank"),
        F.percent_rank().over(w5).alias("budget_percent_rank"),
        F.stddev_samp("dept_name_sum_greatest_budget_scaled_budget").over(w).alias("dept_name_stddev_samp_dept_name_sum_greatest_budget_scaled_budget"),
    )
)

filtered_departments = (
    metrics_departments
    .filter(F.col("greatest_budget_scaled_budget_dense_rank_percent_rank").between(0.55, 0.61))
    .withColumn("budget_share_filled", F.coalesce("budget_share", F.lit(0)))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("DIRECT")))
)

enriched = (
    filtered_departments
    .select(
        "budget",
        "greatest_budget_scaled_budget_dense_rank",
        "dept_name_sum_greatest_budget_scaled_budget",
        "greatest_budget_scaled_budget_dense_rank_percent_rank",
        "dept_name_stddev_samp_dept_name_sum_greatest_budget_scaled_budget",
        "budget_share_filled",
        "dept_name_clean",
        F.dense_rank().over(w6).alias("greatest_budget_scaled_budget_dense_rank_2"),
    )
)

result = (
    enriched
    .select(
        "budget",
        "greatest_budget_scaled_budget_dense_rank_percent_rank",
        "dept_name_stddev_samp_dept_name_sum_greatest_budget_scaled_budget",
        "dept_name_clean",
        "greatest_budget_scaled_budget_dense_rank_2",
    )
)
