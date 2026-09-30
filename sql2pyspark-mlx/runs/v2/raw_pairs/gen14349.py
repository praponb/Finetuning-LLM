from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("pct_berlin") > 56)
    .orderBy("country")
)
