from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("total_rating") < 13)
    .orderBy("country")
)
