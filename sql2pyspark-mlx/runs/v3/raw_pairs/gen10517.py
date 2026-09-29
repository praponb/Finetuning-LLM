from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1567000, 3759000))
    .select(
        "budget",
        F.when(F.col("budget") >= 3766000, "High").when(F.col("budget") >= 1592000, "Medium").otherwise("Low").alias("budget_tier"),
    )
    .orderBy(F.col("budget").desc())
)
