from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("location").isin(["Berlin", "Chiang Mai"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(F.min("budget").alias("min_budget"))
    .filter(F.col("min_budget") < 2628000)
    .select("location_group", "min_budget")
    .orderBy(F.col("min_budget").desc(), "location_group")
)
