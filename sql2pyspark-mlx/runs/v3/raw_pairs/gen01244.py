from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("stddev_salary") >= 38)
)
