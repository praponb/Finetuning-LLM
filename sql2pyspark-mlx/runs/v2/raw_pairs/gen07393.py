from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "budget",
        "location",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
    .orderBy("location", F.col("budget").desc())
)
