from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["London", "Tokyo"]) & F.col("location").isin(["New York", "London"]) & (F.col("budget") < 2892000))
    .select(
        "budget",
        "location",
        "dept_name",
        F.upper("dept_name").alias("dept_name_upper"),
    )
    .orderBy("dept_name")
)
