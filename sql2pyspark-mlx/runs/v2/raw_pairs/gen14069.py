from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("rating") > 3.1)

country_summary = (
    base
    .groupBy("country")
    .agg(
        F.min("rating").alias("min_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)

result = (
    country_summary
    .filter(F.col("avg_rating") > 25)
    .orderBy(F.col("min_rating").desc(), "country")
    .limit(5)
)
