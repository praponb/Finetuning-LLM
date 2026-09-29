from pyspark.sql import functions as F

result = (
    df_departments
    .rollup(
        "location",
        F.when(F.col("location").isin(["Bangkok", "New York"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .orderBy("location_group", F.col("avg_budget").desc())
)
