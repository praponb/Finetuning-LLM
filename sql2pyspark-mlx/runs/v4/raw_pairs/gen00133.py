from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.trim("dept_name").alias("clean_dept_name"),
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
)

result = (
    cleaned
    .select("location", "id")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "HR")
        .select("location", "id")
    )
)
