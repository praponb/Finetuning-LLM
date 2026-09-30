from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.location").isin(["Berlin", "Bangkok"]) & F.col("departments.dept_name").isin(["HR", "Engineering", "Legal"]))
    .select(
        F.col("employees.full_name"),
        F.col("employees.level"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.when(F.col("employees.job_title").isin(["Director", "Intern"]), "Group A").otherwise("Other").alias("job_title_group"),
    )
    .orderBy(F.col("level").desc(), "job_title_group")
)
