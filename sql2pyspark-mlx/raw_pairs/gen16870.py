from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() & (F.col("level") == "L3"))
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
    )
    .filter(F.col("designer_salary") > 79)
)
