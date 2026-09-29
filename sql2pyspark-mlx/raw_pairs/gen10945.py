from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "Chiang Mai", "Bangkok"])
    .agg(F.sum("budget").alias("amount"))
    .select(
        "dept_name",
        F.col("Singapore").alias("singapore"),
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("Bangkok").alias("bangkok"),
    )
)
