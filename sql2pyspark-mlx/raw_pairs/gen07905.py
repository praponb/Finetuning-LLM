from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") != "L2") & F.col("is_remote"))
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
        F.sum("salary").alias("total_salary"),
        F.countDistinct("salary").alias("unique_managers"),
    )
    .orderBy(F.col("total_salary").desc(), F.col("director_salary").desc())
)
