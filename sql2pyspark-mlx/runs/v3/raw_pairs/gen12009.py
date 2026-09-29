from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 163000, "high").when(F.col("salary") >= 43000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("salary_range") > 33)
    .orderBy("salary_bucket")
)
