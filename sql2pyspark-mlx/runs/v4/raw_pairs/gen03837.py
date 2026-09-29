from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy(F.col("level").desc())
    .limit(10)
)
