from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc(), "rating_range", "country")
    .limit(3)
)
