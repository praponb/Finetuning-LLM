from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") != "L5")
    .groupBy("level", "job_title")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
    .filter(F.col("l1_count") < 4)
    .drop("l1_count")
)
