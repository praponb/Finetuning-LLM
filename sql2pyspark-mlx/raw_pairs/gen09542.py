from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([23, 26, 28, 34]))
    .groupBy(
        F.when(F.col("salary") >= 128000, "high").when(F.col("salary") >= 65000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
)
