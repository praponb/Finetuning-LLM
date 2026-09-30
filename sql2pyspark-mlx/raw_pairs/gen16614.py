from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("email").like("%rac%"))
    .groupBy(
        F.when(F.col("salary") >= 132000, "high").when(F.col("salary") >= 84000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
        F.min("hire_date").alias("first_hire_date"),
    )
    .filter(F.col("pct_l3") > 61)
    .orderBy("pct_l3", "first_hire_date")
)
