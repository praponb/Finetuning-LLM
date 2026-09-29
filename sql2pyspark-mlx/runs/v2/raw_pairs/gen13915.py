from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 57000)
    .groupBy("level")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("salary_range") >= 71)
    .select("level", "unique_job_titles", "salary_range")
    .orderBy(F.col("unique_job_titles").desc())
)
