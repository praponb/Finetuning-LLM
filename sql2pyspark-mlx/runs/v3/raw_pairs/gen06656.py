from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Chiang Mai")
    .groupBy(
        F.when(F.col("location").isin(["Chiang Mai", "Berlin"]), "group_a").otherwise("other").alias("location_group"),
        "dept_name",
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)
