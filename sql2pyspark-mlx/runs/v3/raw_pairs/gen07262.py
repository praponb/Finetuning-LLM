from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 3344000) & F.col("dept_name").isNotNull() & (F.col("dept_name") == "Finance"))
    .select(
        "id",
        "budget",
        F.when(F.col("budget") >= 4398000, "High").when(F.col("budget") >= 1088000, "Medium").otherwise("Low").alias("budget_band"),
    )
)
