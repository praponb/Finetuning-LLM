from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .groupBy("country")
    .agg(F.count("rating").alias("rating_count"))
    .filter(F.col("rating_count") > 4)
    .select("country", "rating_count")
    .orderBy("rating_count")
)
