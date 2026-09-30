from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "Singapore"])
    .agg(F.max("budget").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("Berlin_amount").alias("berlin_amount"),
        F.col("Berlin_cnt").alias("berlin_cnt"),
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_cnt").alias("singapore_cnt"),
    )
)
