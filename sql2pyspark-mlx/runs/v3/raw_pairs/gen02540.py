from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("is_remote") & ~F.col("level").isin(["L4", "L1"]))
    .groupBy("job_title", "level")
    .agg(
        F.avg("salary").alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("avg_salary") > 86000)
)
