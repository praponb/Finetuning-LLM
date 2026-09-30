from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2465000)
    .select(
        "location",
        "dept_name",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
    .orderBy(F.col("location").desc())
)
