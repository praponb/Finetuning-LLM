from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(~((F.col("job_title") != "Analyst") & F.col("dept_id").isin([17, 28, 35, 39])))
)

agg = (
    recent_employees
    .groupBy("level")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = agg.filter(F.col("stddev_salary") > 42)
