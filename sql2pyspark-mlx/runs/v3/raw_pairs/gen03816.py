from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "New York") & (F.col("budget") > 1803000))
    .select(
        "id",
        "location",
        "budget",
        F.length("dept_name").alias("dept_name_length"),
        F.floor("budget").alias("budget_whole"),
    )
)
