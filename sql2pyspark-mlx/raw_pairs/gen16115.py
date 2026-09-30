from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .groupBy(
        F.when(F.col("location").isin(["New York", "Tokyo"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count("*").alias("num_departments"),
    )
    .filter(F.col("num_departments") >= 5)
    .drop("num_departments")
    .orderBy("location_group")
)
