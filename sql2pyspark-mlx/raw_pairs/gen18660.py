from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("job_title").isin(["Engineer", "Director"]), "group_a").otherwise("other").alias("job_title_group"),
    )
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .orderBy(F.col("salary_range").desc())
)
