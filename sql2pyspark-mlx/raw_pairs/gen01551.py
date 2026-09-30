from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") == "L3") & F.col("is_remote") & F.col("is_remote"))
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .orderBy("hire_month", F.col("job_title_values").desc())
    .limit(10)
)
