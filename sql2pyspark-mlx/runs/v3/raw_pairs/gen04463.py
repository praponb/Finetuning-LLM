from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .filter(F.col("avg_rating") > 20)
    .orderBy(F.col("avg_rating").desc(), "country")
)
