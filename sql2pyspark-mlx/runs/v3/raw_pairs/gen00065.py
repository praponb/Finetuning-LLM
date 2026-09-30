from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
        F.avg("salary").alias("avg_salary"),
    )
    .orderBy(F.col("pct_l4").desc(), "is_remote")
    .limit(50)
)
