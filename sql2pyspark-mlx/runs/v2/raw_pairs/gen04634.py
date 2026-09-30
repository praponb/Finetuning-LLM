from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
    .filter(F.col("rating_range") >= 41)
    .select("country", "rating_range")
    .orderBy("country", F.col("rating_range").desc())
    .limit(20)
)
