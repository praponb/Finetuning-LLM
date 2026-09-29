from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("budget") < 2194000)
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("Unknown")))
)

result = (
    cleaned
    .groupBy("dept_name")
    .pivot("location", ["Tokyo", "Singapore", "London"])
    .agg(F.sum("budget"))
    .select(
        "dept_name",
        F.col("Tokyo").alias("tokyo"),
        F.col("Singapore").alias("singapore"),
        F.col("London").alias("london"),
    )
)
