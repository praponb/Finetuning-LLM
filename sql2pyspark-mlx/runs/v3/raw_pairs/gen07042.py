from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("budget") > 1190000) & (F.col("location") == "London")) | F.col("budget").between(1736000, 3232000))
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.substring("location", 1, 3).alias("location_prefix"),
    )
)
