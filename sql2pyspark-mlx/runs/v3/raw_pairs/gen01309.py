from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)
