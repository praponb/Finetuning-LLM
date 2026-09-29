from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 2.0) & F.col("rating").between(2.2, 2.3))
    .groupBy("country")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)
