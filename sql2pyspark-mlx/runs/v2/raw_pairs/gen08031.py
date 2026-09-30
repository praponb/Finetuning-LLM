from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNull() | (F.col("job_title") != "Manager"))
    .select(
        "full_name",
        F.when(F.col("salary") >= 146000, "High").when(F.col("salary") >= 51000, "Medium").otherwise("Low").alias("salary_level"),
        F.concat("job_title", F.lit("/"), "level").alias("job_title_level"),
    )
)
