from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Bangkok", "Singapore"]) | F.col("location").isin(["Bangkok", "Berlin"]))
    .select(
        "dept_name",
        "budget",
        F.when(F.col("budget") >= 3601000, "High").when(F.col("budget") >= 939000, "Medium").otherwise("Low").alias("budget_tier"),
    )
    .orderBy("budget_tier")
)
