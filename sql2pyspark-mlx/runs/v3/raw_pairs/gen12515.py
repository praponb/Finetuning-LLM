from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter((F.year("hire_date") >= 2025) & (F.col("level").isin(["L2", "L1"]) | F.col("email").isNotNull()))
    .withColumn("hire_date_minus_30d", F.date_sub("hire_date", 30))
)

summary = (
    recent_employees
    .groupBy(
        F.when(F.col("salary") >= 129000, "high").when(F.col("salary") >= 46000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    summary
    .filter(F.col("avg_salary") >= 40)
    .orderBy(F.col("avg_salary").desc(), "salary_bucket")
    .limit(1)
)
