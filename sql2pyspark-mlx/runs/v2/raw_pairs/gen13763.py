from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 1789000) | ~F.col("dept_name").isin(["Marketing", "Sales"]))
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.when(F.col("budget") >= 3043000, "High").when(F.col("budget") >= 1722000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)
