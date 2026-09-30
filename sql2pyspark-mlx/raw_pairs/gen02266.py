from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("location").orderBy("budget", "id")
w3 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location")
w5 = Window.partitionBy("dept_name", "location")
w6 = Window.orderBy("budget")
w7 = Window.partitionBy("location", "dept_name")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") != "Legal"))
    .withColumn("location_title", F.initcap(F.trim("location")))
    .withColumn("location_clean", F.coalesce("location", F.lit("UNKNOWN")))
)

enriched = (
    cleaned
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_title",
        "location_clean",
        F.avg("budget").over(w).alias("budget_avg_5_prior"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
        F.last("dept_name", ignorenulls=True).over(w3).alias("last_known_dept_name"),
    )
)

remaining_keys = (
    enriched
    .select("id", "location")
    .intersectAll(
        df_departments
        .filter(F.col("dept_name") == "Marketing")
        .select("id", "location")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.round((F.col("budget_avg_5_prior") / F.sum("budget_avg_5_prior").over(w4)), 4).alias("budget_avg_5_prior_share"),
        F.round((F.col("budget") / F.sum("budget").over(w5)), 4).alias("budget_share"),
        F.rank().over(w6).alias("budget_rank"),
        F.count("budget_avg_5_prior").over(w4).alias("location_count_budget_avg_5_prior"),
        F.avg("budget").over(w7).alias("location_avg_budget"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.lower(F.trim(F.col("x.full_name"))) == "david lee"), "left_anti")
)

result = (
    eligible
    .select("location", "budget_avg_5_prior_share", "prev_budget", "budget")
    .unpivot(
        ids=["location"],
        values=["budget_avg_5_prior_share", "prev_budget", "budget"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
