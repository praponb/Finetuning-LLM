from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") == "L2") & F.col("manager_id").isNull())
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
        F.countDistinct("salary").alias("unique_depts"),
    )
    .filter(F.col("unique_depts") < 3)
    .drop("unique_depts")
    .orderBy(F.col("intern_count").desc())
)
