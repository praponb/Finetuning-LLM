from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Bangkok", "Berlin", "London"])
    .agg(F.max("budget").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("Bangkok_total").alias("bangkok_total"),
        F.col("Bangkok_cnt").alias("bangkok_cnt"),
        F.col("Berlin_total").alias("berlin_total"),
        F.col("Berlin_cnt").alias("berlin_cnt"),
        F.col("London_total").alias("london_total"),
        F.col("London_cnt").alias("london_cnt"),
    )
)
