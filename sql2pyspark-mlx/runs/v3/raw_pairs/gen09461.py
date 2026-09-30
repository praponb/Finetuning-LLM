from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("location").isin(["Tokyo", "New York"]), "core").otherwise("other").alias("location_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
    .filter(F.col("pct_singapore") >= 63)
    .select("location_group", "pct_singapore")
)
