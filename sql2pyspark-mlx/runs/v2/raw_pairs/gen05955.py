from pyspark.sql import functions as F

result = (
    df_employees
    .rollup("level", "job_title")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy("job_title", "level")
)
