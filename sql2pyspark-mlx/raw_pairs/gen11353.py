from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Singapore", "London"]) & F.col("location").isNull() & (F.col("location") == "Singapore"))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.initcap(F.trim("location")).alias("location_title"),
    )
)
