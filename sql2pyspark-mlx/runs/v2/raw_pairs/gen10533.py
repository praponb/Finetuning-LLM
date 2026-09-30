from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNull() & ((F.col("job_title") == "Director") | F.col("is_remote")))
    .groupBy(
        F.when(F.col("salary") >= 144000, "high").when(F.col("salary") >= 73000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sum("salary").alias("total_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
