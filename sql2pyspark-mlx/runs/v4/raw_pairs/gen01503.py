from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("budget") >= 3689000)
    .select(
        "*",
        F.when(F.col("location") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
        F.round((F.col("budget") * 100), 2).alias("budget_scaled"),
    )
)

result = (
    cleaned
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Sales"])
    .agg(F.max("budget_scaled"))
    .select(
        "location",
        F.col("Engineering").alias("engineering"),
        F.col("Sales").alias("sales"),
    )
)
