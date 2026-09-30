from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") < 13) & F.col("dept_id").isin([5, 6]))
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 127000, "high").when(F.col("salary") >= 50000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .filter(F.col("pct_l3") < 35)
)
