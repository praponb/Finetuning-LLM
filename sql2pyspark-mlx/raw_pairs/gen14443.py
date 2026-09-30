from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_tier", F.when(F.col("budget") >= 4307000, "High").when(F.col("budget") >= 1168000, "Medium").otherwise("Low"))
    .withColumn("budget_level", F.when(F.col("budget") >= 4403000, "High").when(F.col("budget") >= 1445000, "Medium").otherwise("Low"))
)

result = (
    filtered
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("location").isin(["Singapore", "New York"]))
        .select("dept_name", "location")
    )
)
