from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.count("*").alias("cnt"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .filter(F.col("rating_range") >= 50)
    .orderBy("country")
)
