from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("is_marketing", F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))
)

departments_stats = (
    base
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
    )
)

result = (
    departments_stats
    .select("dept_name", "marketing_count")
    .orderBy("dept_name")
)
