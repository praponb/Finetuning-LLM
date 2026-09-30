from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("avg_rating") < 43)
    .orderBy(F.col("total_rating").desc(), "city")
    .limit(5)
)
