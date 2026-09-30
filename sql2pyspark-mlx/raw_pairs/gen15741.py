from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1958000)
    .groupBy(
        F.when(F.col("location").isin(["New York", "Singapore"]), "priority").otherwise("other").alias("location_group"),
        "dept_name",
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)
