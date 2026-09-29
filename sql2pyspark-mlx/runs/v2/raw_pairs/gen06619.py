from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.ceil("budget").alias("budget_whole"),
        F.concat("dept_name", F.lit(" - "), "location").alias("dept_name_location"),
    )
)
