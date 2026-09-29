from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Singapore", "New York"]))
    .select(
        "*",
        F.upper("dept_name").alias("dept_name_upper"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
)

result = (
    filtered
    .groupBy("dept_name")
    .pivot("location", ["London", "Singapore", "Berlin"])
    .agg(F.avg("budget").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "dept_name",
        F.col("London_amount").alias("london_amount"),
        F.col("London_item_count").alias("london_item_count"),
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
        F.col("Berlin_amount").alias("berlin_amount"),
        F.col("Berlin_item_count").alias("berlin_item_count"),
    )
)
