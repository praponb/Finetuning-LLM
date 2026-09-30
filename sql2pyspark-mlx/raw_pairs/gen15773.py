from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["HR", "Sales", "Engineering"])
    .agg(F.max("budget"))
    .select(
        "location",
        F.col("HR").alias("hr"),
        F.col("Sales").alias("sales"),
        F.col("Engineering").alias("engineering"),
    )
)
