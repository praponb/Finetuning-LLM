from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location")
w_recent = Window.partitionBy("location").orderBy(F.col("budget").desc())
w4 = Window.orderBy("running_budget")

with_windows = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.sum("budget").over(w).alias("running_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.dense_rank().over(w_recent).alias("budget_dense_rank"),
    )
)

remaining_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_departments
        .filter(F.col("dept_name") == "Marketing")
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("budget_dense_rank").between(3, 5))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("budget_dense_rank_share", F.round((F.col("budget_dense_rank") / F.sum("budget_dense_rank").over(w2)), 4))
    .withColumn("running_budget_rank", F.rank().over(w4))
    .withColumn("running_budget_share", F.round((F.col("running_budget") / F.sum("running_budget").over(w2)), 4))
)

kept = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.is_remote")), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("location").isNotNull() & F.col("running_budget_share").isNotNull())
    .select(
        "location",
        "budget",
        "running_budget",
        "budget_dense_rank",
        "budget_share_2",
        "running_budget_rank",
        "running_budget_share",
        F.upper("location").alias("location_upper"),
    )
)

base = (
    cleaned
    .filter(F.col("location").isNotNull())
    .select(
        "budget",
        "running_budget",
        "budget_dense_rank",
        "budget_share_2",
        "running_budget_rank",
        F.round((F.col("budget_share_2") / F.col("running_budget")), 2).alias("budget_share_2_per_running_budget"),
    )
)

result = (
    base
    .select(
        "budget",
        "budget_dense_rank",
        "budget_share_2",
        "running_budget_rank",
        "budget_share_2_per_running_budget",
    )
    .orderBy(F.col("budget_share_2").desc())
)
