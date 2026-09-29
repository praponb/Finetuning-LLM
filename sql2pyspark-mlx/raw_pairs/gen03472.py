from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "dept_id").orderBy("department_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id", "manager_id")
w4 = Window.partitionBy(F.date_trunc("week", "hire_date"), "department_dept_name").orderBy("department_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(~F.col("level").eqNullSafe(F.lit("L1")))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "hire_date",
        "level",
        F.upper("email").alias("email_upper"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") >= 2416000)
    .select("a.*", F.col("d.dept_name"), F.col("d.location"), F.col("d.budget"))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.dept_name").alias("department_dept_name"),
        F.col("b.budget").alias("department_budget"),
        F.col("b.location").alias("department_location"),
    )
)

enriched_employees = (
    with_dims
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w))
    .withColumn("cumulative_budget", F.sum("budget").over(w2))
    .withColumn("department_budget_share", F.round((F.col("department_budget") / F.sum("department_budget").over(w3)), 4))
    .withColumn("cumulative_department_budget", F.sum("department_budget").over(w4))
)

result = (
    enriched_employees
    .groupBy("dept_name")
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.sum(F.when(F.col("location") == "New York", F.col("department_budget_share")).otherwise(0)).alias("new_york_department_budget_share"),
    )
    .orderBy(F.col("num_dept_names").desc_nulls_last())
)
