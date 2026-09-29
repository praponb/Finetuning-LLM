from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget_int").desc_nulls_first())
w_recent = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("dept_name").orderBy("budget_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("location").orderBy("budget", "id")
w6 = Window.partitionBy("dept_name")
w7 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.upper("dept_name").alias("dept_name_upper"),
        F.coalesce("location", F.lit("NONE")).alias("location_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("location") == "London")
    .select(
        "id",
        "dept_name",
        "budget",
        "budget_int",
        "dept_name_upper",
        "location_filled",
        F.cume_dist().over(w).alias("budget_int_cume_dist"),
        F.last("location", ignorenulls=True).over(w_recent).alias("last_known_location"),
        F.sum("budget").over(w_ordered).alias("running_budget"),
        F.last("budget", ignorenulls=True).over(w4).alias("last_known_budget"),
        F.lead("budget", 2, 0).over(w_part).alias("next_budget"),
    )
)

valid = (
    with_windows
    .filter(F.col("dept_name").isNotNull() & (F.col("budget_int_cume_dist") > 0.72))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("NONE")))
)

valid_departments = (
    valid
    .filter(F.col("budget_int") != 51)
    .withColumn("last_known_budget_plus_next_budget", (F.col("last_known_budget") + F.col("next_budget")))
)

kept = (
    valid_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.is_remote")), "left_anti")
)

metrics = (
    kept
    .select(
        "*",
        F.round((F.col("last_known_budget") / F.sum("last_known_budget").over(w6)), 4).alias("last_known_budget_share"),
        F.avg("last_known_budget_plus_next_budget").over(w7).alias("last_known_budget_plus_next_budget_avg_5"),
    )
)

result = (
    metrics
    .select(
        "id",
        "dept_name",
        "budget_int",
        "dept_name_upper",
        "location_filled",
        "budget_int_cume_dist",
        "running_budget",
        "last_known_budget",
        "dept_name_clean",
    )
)
