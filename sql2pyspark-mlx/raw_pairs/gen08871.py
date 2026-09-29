from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") & (F.col("budget") < 1429000) & (F.col("dept_name") != "Engineering"))
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.trim("location").alias("clean_location"),
    )
    .orderBy(F.col("id").desc())
)
