from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2361000, 3492000))
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.trim("location").alias("clean_location"),
        F.concat_ws("/", "location", "dept_name").alias("location_dept_name_key"),
    )
    .orderBy("dept_name", "id")
    .limit(3)
)
