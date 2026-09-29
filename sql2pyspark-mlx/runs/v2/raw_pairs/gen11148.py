from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.year("hire_date") >= 2024)
    .select(
        "id",
        "salary",
        "email",
        F.concat("job_title", F.lit(", "), "email").alias("job_title_email"),
        F.when(F.col("salary") >= 126000, "High").when(F.col("salary") >= 74000, "Medium").otherwise("Low").alias("salary_band"),
    )
)
