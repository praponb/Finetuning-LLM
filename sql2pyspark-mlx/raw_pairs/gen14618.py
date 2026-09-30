from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("location").orderBy("running_min_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location").orderBy("budget_share", "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_level", F.when(F.col("budget") >= 3402000, "High").when(F.col("budget") >= 1568000, "Medium").otherwise("Low"))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level").isin(["L1", "L5"])), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "id",
        "location",
        "budget",
        "budget_level",
        F.min("budget").over(w_recent).alias("running_min_budget"),
        F.max("budget").over(w_recent).alias("running_max_budget"),
        F.sum("budget").over(w_recent).alias("running_sum_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
    )
)

scored = (
    metrics
    .filter(F.col("budget").isNull())
    .select(
        "*",
        F.count("running_max_budget").over(w3).alias("running_count_running_max_budget"),
        F.min("running_max_budget").over(w3).alias("running_min_running_max_budget"),
        F.sum("running_max_budget").over(w3).alias("running_sum_running_max_budget"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "location",
        "budget",
        "budget_level",
        "running_min_budget",
        "budget_share",
        "running_min_running_max_budget",
        "latest_rank",
    )
)
