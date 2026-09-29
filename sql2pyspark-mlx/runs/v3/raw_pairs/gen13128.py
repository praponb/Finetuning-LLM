from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("location").isin(["Chiang Mai", "Berlin"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(F.min("budget").alias("min_budget"))
)
