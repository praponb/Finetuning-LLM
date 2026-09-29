from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter((F.col("departments.location") == "Berlin") & F.col("departments.budget").between(1093000, 3162000))
    .select(
        F.col("employees.hire_date"),
        F.col("employees.email"),
        F.col("departments.location"),
        F.when(F.col("employees.salary") >= 122000, "High").when(F.col("employees.salary") >= 67000, "Medium").otherwise("Low").alias("salary_level"),
    )
    .orderBy("salary_level", F.col("hire_date").desc())
)
