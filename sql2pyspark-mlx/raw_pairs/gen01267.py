from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(119000, 146000))
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .filter(F.col("director_salary") >= 39)
)
