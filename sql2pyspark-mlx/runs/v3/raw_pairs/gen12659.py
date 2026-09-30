from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city", "country")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.min("rating").alias("min_rating"),
    )
)
