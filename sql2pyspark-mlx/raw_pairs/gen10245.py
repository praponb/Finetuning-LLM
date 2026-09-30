from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("budget").between(3139000, 3638000) & F.col("location").isNotNull())
)

agg = (
    valid_departments
    .groupBy("dept_name", "location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
    )
)

result = (
    agg
    .select("dept_name", "location", "engineering_budget")
    .orderBy(F.col("location").desc())
)
