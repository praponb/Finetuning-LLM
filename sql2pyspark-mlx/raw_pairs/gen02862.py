from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy(F.col("hire_date").desc())
w_ordered = Window.partitionBy("dept_id", "dept_name").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("email_lower", F.lower("email"))
    .withColumn("level_filled", F.coalesce("level", F.lit("unassigned")))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "email_lower",
        "level_filled",
    )
)

enriched = (
    deduped
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.dept_id",
        "a.salary",
        "a.hire_date",
        "a.level_filled",
        F.col("p.dept_name"),
        F.col("p.budget"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "budget",
        F.count("budget").over(w_ordered).alias("running_count_budget"),
        F.min("budget").over(w_ordered).alias("running_min_budget"),
        F.max("budget").over(w_ordered).alias("running_max_budget"),
    )
)

result = (
    scored
    .select("id", "budget", "running_min_budget", "running_max_budget")
    .orderBy(F.col("running_min_budget").desc_nulls_first(), "id")
)
