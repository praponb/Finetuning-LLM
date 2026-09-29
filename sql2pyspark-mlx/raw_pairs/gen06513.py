from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-13").cast("date")) & (F.col("hire_date") > "2023-07-01"))
    .select(
        "*",
        F.when(F.col("salary") >= 163000, "High").when(F.col("salary") >= 85000, "Medium").otherwise("Low").alias("salary_level"),
        F.coalesce("full_name", F.lit("unassigned")).alias("full_name_filled"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_departments.alias("dim"), F.col("e.dept_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["location", "job_title"], ["location"], []],
        F.col("dim.location").alias("location"),
        F.col("e.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.salary")).alias("max_salary"),
    )
    .orderBy(F.col("max_salary").asc_nulls_last())
)
