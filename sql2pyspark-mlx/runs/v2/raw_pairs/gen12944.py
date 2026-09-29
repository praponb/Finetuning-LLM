from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2728000) & ~F.col("dept_name").isin(["Finance", "Sales"]))
    .select(
        "budget",
        "dept_name",
        "location",
        F.upper("location").alias("location_upper"),
        F.upper("location").alias("location_upper_2"),
    )
)
