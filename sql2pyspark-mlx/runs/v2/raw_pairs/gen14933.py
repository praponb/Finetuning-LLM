from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.when(F.col("budget") >= 3236000, "High").when(F.col("budget") >= 1220000, "Medium").otherwise("Low").alias("budget_level"),
    )
    .orderBy("dept_name", F.col("budget_level").desc(), "id")
    .limit(10)
)
