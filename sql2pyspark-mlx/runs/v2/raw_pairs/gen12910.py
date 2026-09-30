from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") > 11) & (F.col("level") == "L5") & (F.year("hire_date") >= 2023))
    .select(
        "id",
        "job_title",
        "manager_id",
        "is_remote",
        F.concat("full_name", F.lit(" "), "level").alias("full_name_level"),
    )
    .orderBy(F.col("full_name_level").desc())
)
