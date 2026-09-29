from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("country") != "TH")

country_summary = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.max("rating").alias("max_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)

result = (
    country_summary
    .select("country", "max_rating", "avg_rating")
    .orderBy(F.col("avg_rating").desc())
)
