from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("rating") > 4.0)

suppliers_stats = (
    src
    .groupBy("city")
    .agg(F.min("rating").alias("min_rating"))
)

result = (
    suppliers_stats
    .filter(F.col("min_rating") >= 3.2)
    .select("city", "min_rating")
)
