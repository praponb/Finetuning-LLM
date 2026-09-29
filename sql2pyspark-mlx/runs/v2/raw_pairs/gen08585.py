from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull() & (F.col("dept_name") == "Sales"))
    .select(
        "id",
        "budget",
        "dept_name",
        F.length("location").alias("location_length"),
        F.when(F.col("budget") >= 3798000, "High").when(F.col("budget") >= 1729000, "Medium").otherwise("Low").alias("budget_level"),
    )
)
