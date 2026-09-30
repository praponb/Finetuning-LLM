from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.when(F.col("budget") >= 3659000, "High").when(F.col("budget") >= 710000, "Medium").otherwise("Low").alias("budget_tier"),
        F.when(F.col("location") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
    )
)
