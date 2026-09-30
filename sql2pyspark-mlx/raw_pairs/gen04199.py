from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Finance", "Sales"])
    .agg(F.max("budget").alias("total"))
    .select(
        "location",
        F.col("Finance").alias("finance"),
        F.col("Sales").alias("sales"),
    )
)
