from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Sales", "Engineering"]) & (F.col("budget") > 1351000))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.concat_ws("/", "location", "dept_name").alias("location_dept_name_key"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)
