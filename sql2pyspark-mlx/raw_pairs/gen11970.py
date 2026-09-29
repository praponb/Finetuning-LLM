from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc_nulls_first())
w2 = Window.orderBy("budget_cume_dist")
w3 = Window.partitionBy("location").orderBy(F.col("budget").desc())
w4 = Window.orderBy("budget")
w5 = Window.partitionBy("location").orderBy(F.col("budget_cume_dist").asc_nulls_first())
w6 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first())
w7 = Window.orderBy(F.col("budget_cume_dist_3").asc_nulls_first())

kept_keys = (
    df_departments
    .select("dept_name")
    .exceptAll(
        df_departments
        .filter(F.col("location").isin(["Bangkok", "London"]))
        .select("dept_name")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("dept_name", "id")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Marketing")
        .select("dept_name", "id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "dept_name",
        "location",
        "budget",
        F.cume_dist().over(w).alias("budget_cume_dist"),
    )
)

metrics = (
    with_windows
    .select(
        "dept_name",
        "location",
        "budget_cume_dist",
        F.cume_dist().over(w2).alias("budget_cume_dist_cume_dist"),
        F.cume_dist().over(w3).alias("budget_cume_dist_2"),
        F.rank().over(w4).alias("budget_rank"),
        F.cume_dist().over(w5).alias("budget_cume_dist_cume_dist_2"),
        F.cume_dist().over(w6).alias("budget_cume_dist_3"),
    )
)

metrics_departments = (
    metrics
    .select("*", F.percent_rank().over(w7).alias("budget_cume_dist_3_percent_rank"))
)

result = (
    metrics_departments
    .select(
        "dept_name",
        "location",
        "budget_cume_dist_cume_dist",
        "budget_cume_dist_2",
        "budget_cume_dist_cume_dist_2",
        "budget_cume_dist_3_percent_rank",
    )
)
