from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3281000, 3692000) | F.col("dept_name").isin(["Engineering", "Marketing", "Finance"]))
    .groupBy(
        F.when(F.col("dept_name").isin(["Engineering", "Marketing"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.min("budget").alias("min_budget"),
        F.count("*").alias("department_count"),
    )
    .filter(F.col("department_count") < 1)
    .drop("department_count")
    .orderBy(F.col("chiang_mai_count").desc())
)
