from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("budget") > 1825000) & (F.col("budget") < 3003000)) | F.col("location").isNotNull())
    .groupBy("dept_name")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
    )
    .filter(F.col("stddev_budget") > 52)
)
