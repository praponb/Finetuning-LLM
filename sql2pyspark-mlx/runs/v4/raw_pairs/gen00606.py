from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id")

with_dims = (
    df_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.location"))
)

valid = (
    with_dims
    .filter(F.col("hire_date") >= F.to_date(F.lit("2024-02-19")))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.round((F.col("salary") * 1.15), 2).alias("adjusted_salary"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims_employees = (
    latest
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.budget"), F.col("p.location"))
)

result = (
    with_dims_employees
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date"))
    .groupingSets(
        [["activity_month", "dept_id"], ["activity_month"]],
        "activity_month",
        "dept_id",
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_rows"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("location") == "Berlin", 1)), F.lit(0)), 2).alias("salary_per_berlin"),
        F.round(F.avg("is_remote_flag"), 2).alias("avg_is_remote_flag"),
    )
    .filter(F.col("avg_is_remote_flag") > 0)
)
