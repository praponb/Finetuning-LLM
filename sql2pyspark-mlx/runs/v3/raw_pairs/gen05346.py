from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1619000)
    .select(
        "budget",
        "location",
        F.lower("dept_name").alias("dept_name_lower"),
        F.when(F.col("budget") >= 3660000, "High").when(F.col("budget") >= 2017000, "Medium").otherwise("Low").alias("budget_band"),
    )
)
