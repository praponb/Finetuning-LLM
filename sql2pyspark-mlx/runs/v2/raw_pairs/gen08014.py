from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2252000) & (F.col("dept_name") == "Sales"))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.length("location").alias("location_length"),
        F.when(F.col("budget") >= 3053000, "High").when(F.col("budget") >= 1016000, "Medium").otherwise("Low").alias("budget_band"),
    )
    .orderBy(F.col("location_length").desc())
)
