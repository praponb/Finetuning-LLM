from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .select(F.lit("session").alias("record_type"), "country")
    .union(df_products.select(F.lit("product").alias("record_type"), "brand"))
    .distinct()
)
