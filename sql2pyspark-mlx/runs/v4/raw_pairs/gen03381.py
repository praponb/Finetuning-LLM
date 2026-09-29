from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Tokyo", "New York"]))
    .select(
        "id",
        "budget",
        "dept_name",
        F.when(F.col("dept_name").isin(["Sales", "Finance"]), "Tier 1").otherwise("Other").alias("dept_name_group"),
    )
    .orderBy("dept_name", "id")
    .limit(3)
)
