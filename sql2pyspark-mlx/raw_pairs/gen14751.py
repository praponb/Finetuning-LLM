from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("is_remote"))
    .withColumn("salary_int", F.col("salary").cast("int"))
)

summary = (
    src
    .groupBy(
        F.when(F.col("job_title").isin(["Analyst", "Manager"]), "group_a").otherwise("other").alias("job_title_group"),
    )
    .agg(F.min("salary_int").alias("min_salary_int"))
)

result = (
    summary
    .filter(F.col("min_salary_int") > 44)
    .orderBy("job_title_group")
)
