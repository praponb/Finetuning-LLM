from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([13, 15, 23, 34]))
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 49000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.max("salary").alias("max_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .filter(F.col("max_salary") > 124000)
    .orderBy("job_title_values", "job_title", "salary_bucket")
    .limit(5)
)
