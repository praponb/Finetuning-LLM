from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull())
    .groupBy("country")
    .agg(
        F.max("rating").alias("max_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("max_rating") >= 2.8)
)
