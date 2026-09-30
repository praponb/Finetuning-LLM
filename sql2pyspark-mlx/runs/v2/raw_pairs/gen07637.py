from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
    .filter(F.col("rating_range") < 46)
    .select("country", "city", "rating_range")
    .orderBy(F.col("rating_range").desc(), F.col("city").desc(), "country")
    .limit(1)
)
