from pyspark.sql import functions as F

recent_employees = df_employees.filter(F.lower(F.trim("email")) == "carol@company.co")

agg = (
    recent_employees
    .groupBy(
        F.when(F.col("salary") >= 140000, "high").when(F.col("salary") >= 49000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"))
)

result = (
    agg
    .filter(F.col("l4_count") >= 58)
    .select("salary_bucket", "l4_count")
    .orderBy(F.col("salary_bucket").desc())
    .limit(3)
)
