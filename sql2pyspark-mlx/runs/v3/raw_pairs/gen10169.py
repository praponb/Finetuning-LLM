from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 122000, "high").when(F.col("salary") >= 78000, "mid").otherwise("low").alias("salary_bucket"),
        "level",
    )
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") >= 52)
)
