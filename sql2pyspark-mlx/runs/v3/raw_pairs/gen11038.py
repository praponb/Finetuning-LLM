from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([12, 18, 26]) & (F.col("job_title") != "Analyst"))
    .select(
        "id",
        "email",
        "level",
        "is_remote",
        F.when(F.col("salary") >= 131000, "High").when(F.col("salary") >= 65000, "Medium").otherwise("Low").alias("salary_band"),
    )
)
