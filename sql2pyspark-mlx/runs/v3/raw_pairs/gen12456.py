from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Chiang Mai") & F.col("location").isin(["New York", "Berlin"]))
    .groupBy(
        "dept_name",
        F.when(F.col("dept_name").isin(["Finance", "Sales"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
)
