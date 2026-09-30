from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") < 14)
    .groupBy(
        F.when(F.col("job_title").isin(["Intern", "Engineer"]), "core").otherwise("other").alias("job_title_group"),
    )
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .orderBy(F.col("stddev_salary").desc())
)
