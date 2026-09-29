from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 1))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("OTHER")))
)

result = (
    base
    .groupBy("dept_name")
    .pivot("location", ["Tokyo", "Bangkok", "London"])
    .agg(F.sum("budget_filled").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "dept_name",
        F.col("Tokyo_amount").alias("tokyo_amount"),
        F.col("Tokyo_item_count").alias("tokyo_item_count"),
        F.col("Bangkok_amount").alias("bangkok_amount"),
        F.col("Bangkok_item_count").alias("bangkok_item_count"),
        F.col("London_amount").alias("london_amount"),
        F.col("London_item_count").alias("london_item_count"),
    )
)
