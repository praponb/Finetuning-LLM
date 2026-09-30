from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-04-26", "2024-08-01"))
    .groupBy(
        F.when(F.col("salary") >= 129000, "high").when(F.col("salary") >= 77000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .orderBy("stddev_salary")
)
