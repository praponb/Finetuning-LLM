from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .select(
        "id",
        "budget",
        "location",
        F.when(F.col("budget") >= 4221000, "High").when(F.col("budget") >= 778000, "Medium").otherwise("Low").alias("budget_level"),
    )
    .orderBy(F.col("location").desc(), "id")
    .limit(10)
)
