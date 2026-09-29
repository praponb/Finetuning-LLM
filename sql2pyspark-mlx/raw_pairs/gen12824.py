from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name").isin(["Sales", "Legal", "Marketing"]) & (F.upper(F.trim(F.col("e.email"))) == "BOB.SMITH@YAHOO.COM"))
    .select(
        F.col("e.id"),
        F.col("e.full_name"),
        F.col("e.hire_date"),
        F.col("e.job_title"),
        F.col("d.budget"),
        F.col("d.location"),
        F.initcap(F.trim(F.col("e.job_title"))).alias("job_title_title"),
    )
    .orderBy(F.col("id").desc())
)
