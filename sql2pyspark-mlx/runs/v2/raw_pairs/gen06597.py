from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(~F.col("is_remote") & (F.col("salary") < 128000)))
    .groupBy(
        "is_remote",
        F.when(F.col("salary") >= 158000, "high").when(F.col("salary") >= 69000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
