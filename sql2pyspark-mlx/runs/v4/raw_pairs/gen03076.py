from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.count("*").alias("cnt"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .filter(F.col("budget_stddev") > 0)
    .orderBy(F.col("cnt").desc())
)
