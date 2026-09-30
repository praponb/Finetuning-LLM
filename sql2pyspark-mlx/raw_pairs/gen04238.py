from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & F.col("dept_name").isin(["Marketing", "Engineering"]) & F.col("location").isNotNull())
    .select(
        "id",
        "budget",
        F.when(F.col("budget") >= 4287000, "High").when(F.col("budget") >= 1136000, "Medium").otherwise("Low").alias("budget_level"),
    )
    .orderBy("budget_level", "id")
    .limit(5)
)
