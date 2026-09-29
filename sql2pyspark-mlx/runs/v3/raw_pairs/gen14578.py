from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Chiang Mai", "Berlin"]) & F.col("location").isNull())
    .select(
        "dept_name",
        "location",
        "budget",
        F.upper("dept_name").alias("dept_name_upper"),
    )
)
