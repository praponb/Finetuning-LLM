from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") < 7)
    .select(
        "is_remote",
        "salary",
        "dept_id",
        F.initcap(F.trim("full_name")).alias("full_name_title"),
    )
)
