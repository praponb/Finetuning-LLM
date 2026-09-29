from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "is_remote",
        F.when(F.col("job_title").isin(["Designer", "Engineer"]), "group_a").otherwise("other").alias("job_title_group"),
    )
    .agg(F.avg("salary").alias("avg_salary"))
    .orderBy(F.col("avg_salary").desc(), "job_title_group")
)
