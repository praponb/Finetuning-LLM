from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(75000, 112000))
    .groupBy(
        F.when(F.col("job_title").isin(["Designer", "Intern"]), "priority").otherwise("other").alias("job_title_group"),
    )
    .agg(F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"))
    .filter(F.col("l5_count") > 64)
    .select("job_title_group", "l5_count")
)
