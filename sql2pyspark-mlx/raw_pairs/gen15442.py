from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("budget") <= 1695000)

summary = (
    recent_departments
    .groupBy(
        F.when(F.col("location").isin(["Berlin", "Singapore"]), "group_a").otherwise("other").alias("location_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
)

result = (
    summary
    .filter(F.col("pct_singapore") < 60)
    .select("location_group", "pct_singapore")
    .orderBy(F.col("location_group").desc())
)
