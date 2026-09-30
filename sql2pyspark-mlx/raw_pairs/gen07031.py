from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "New York"])
    .agg(F.max("budget").alias("amount"))
    .select(
        "dept_name",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("New York").alias("new_york"),
    )
)
