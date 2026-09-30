from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("New York"))))
    .withColumn("budget_band", F.when(F.col("budget") >= 4406000, "High").when(F.col("budget") >= 1972000, "Medium").otherwise("Low"))
)

valid = (
    filtered
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("London"))))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_band",
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("resolved_dept_name"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isin([6, 8, 26])), "left_anti")
)

latest = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_latest = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"), F.col("c.level"))
)

result = with_latest.select("id", "dept_name", "budget", "budget_band", "resolved_dept_name")
