from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        F.lower("job_title").alias("job_title_lower"),
        F.when(F.col("salary") >= 143000, "High").when(F.col("salary") >= 65000, "Medium").otherwise("Low").alias("salary_band"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.dept_id",
        "s.manager_id",
        "s.hire_date",
        "s.level",
        "s.level_filled",
        F.col("p.location"),
        F.col("p.dept_name"),
        F.col("p.budget"),
    )
)
