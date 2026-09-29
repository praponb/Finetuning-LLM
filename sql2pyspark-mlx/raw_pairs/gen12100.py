from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("budget") >= 3089000)
    .select(
        "*",
        F.round((F.col("budget") * 0.9), 2).alias("budget_adjusted"),
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

metrics = (
    valid
    .filter(F.col("budget").isNotNull())
    .select(
        "*",
        F.min("budget_adjusted").over(w_hist).alias("last3_min_budget_adjusted"),
        F.avg("budget_adjusted").over(w_hist).alias("last3_avg_budget_adjusted"),
        F.count("budget_adjusted").over(w_hist).alias("last3_count_budget_adjusted"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.email").isNotNull()), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        "budget_adjusted",
        "dept_name_prefix",
        "dept_name_filled",
        "last3_min_budget_adjusted",
        "last3_avg_budget_adjusted",
        "last3_count_budget_adjusted",
    )
    .orderBy(
        F.col("last3_avg_budget_adjusted").desc_nulls_first(),
        "last3_min_budget_adjusted",
    )
)
