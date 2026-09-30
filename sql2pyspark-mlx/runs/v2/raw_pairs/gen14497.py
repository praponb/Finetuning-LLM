from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 157000, "high").when(F.col("salary") >= 75000, "mid").otherwise("low").alias("salary_bucket"),
        "dept_id",
    )
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") > 47)
    .orderBy("salary_bucket", F.col("dept_id").desc())
)
