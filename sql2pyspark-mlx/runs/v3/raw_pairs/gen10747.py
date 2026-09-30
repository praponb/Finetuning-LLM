from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "is_remote")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") < 78000)
    .drop("avg_salary")
    .orderBy(F.col("pct_l4").desc())
)
