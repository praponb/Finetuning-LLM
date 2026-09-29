from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Engineering", "Sales"]) & F.col("location").isNotNull())
    .select(
        "id",
        "budget",
        "dept_name",
        F.floor("budget").alias("budget_whole"),
        F.when(F.col("location") == "London", 1).otherwise(0).alias("is_london"),
    )
)
