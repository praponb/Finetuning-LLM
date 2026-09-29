from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.max("rating").alias("max_rating"),
    )
    .orderBy("max_rating")
)
