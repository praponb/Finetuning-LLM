from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.round((F.col("budget") * 1.1), 2).alias("adjusted_budget"),
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
    )
    .orderBy("location_dept_name_key")
)
