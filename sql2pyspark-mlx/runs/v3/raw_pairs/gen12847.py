from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.lower(F.trim("email")) == "carol@company.co") & F.col("manager_id").isNotNull())
    .select(
        "dept_id",
        "level",
        "salary",
        F.concat("full_name", F.lit("/"), "level").alias("full_name_level"),
    )
    .orderBy(F.col("dept_id").desc())
)
