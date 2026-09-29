from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") & (F.col("dept_name") == "Marketing") & F.col("budget").isNotNull())
    .select(
        "id",
        "location",
        "budget",
        F.lower("location").alias("location_lower"),
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
    )
    .orderBy("budget", F.col("location_lower").desc())
)
