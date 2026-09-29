from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Sup%") & ((F.col("country") == "TH") | (F.col("city") == "Berlin")))
    .groupBy("country")
    .agg(
        F.count("*").alias("num_suppliers"),
        F.max("rating").alias("max_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .orderBy("max_rating")
)
