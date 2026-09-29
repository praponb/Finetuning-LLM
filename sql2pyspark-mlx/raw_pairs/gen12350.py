from pyspark.sql import functions as F

result = (
    df_employees
    .rollup(
        F.when(F.col("salary") >= 136000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("l3_count") > 1)
    .orderBy("l3_count")
)
