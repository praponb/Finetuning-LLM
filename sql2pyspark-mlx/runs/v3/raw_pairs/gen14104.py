from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .orderBy("avg_rating", "country")
    .limit(1)
)
