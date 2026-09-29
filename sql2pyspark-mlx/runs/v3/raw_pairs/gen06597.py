from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("rating") > 3.5)

country_summary = (
    valid_suppliers
    .groupBy("country")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)

result = (
    country_summary
    .filter(F.col("avg_rating") > 74)
    .select("country", "avg_rating")
)
