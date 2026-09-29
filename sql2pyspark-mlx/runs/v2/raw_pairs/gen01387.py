from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "London", "Singapore"]) & (F.col("dept_name") == "Finance"))
    .select(
        "dept_name",
        F.when(F.col("budget") >= 3189000, "High").when(F.col("budget") >= 1290000, "Medium").otherwise("Low").alias("budget_band"),
    )
    .orderBy("budget_band")
)
