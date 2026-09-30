from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc(), "location")
    .limit(20)
)
