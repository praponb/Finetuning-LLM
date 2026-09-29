from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("city") == "Singapore")

agg = (
    filtered_suppliers
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
)

result = (
    agg
    .filter(F.col("rating_count") >= 4)
    .orderBy("rating_range")
)
