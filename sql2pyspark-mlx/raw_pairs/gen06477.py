from pyspark.sql import functions as F

result = (
    df_employees
    .filter(((F.year("hire_date") == 2023) & F.month("hire_date").isin([5, 8, 9])) | (F.col("salary") >= 113000))
    .groupBy(
        F.when(F.col("salary") >= 146000, "high").when(F.col("salary") >= 50000, "mid").otherwise("low").alias("salary_bucket"),
        "is_remote",
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.min("hire_date").alias("min_hire_date"),
    )
    .filter(F.col("stddev_salary") < 35)
    .orderBy("min_hire_date")
)
