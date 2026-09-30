from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.lower(F.trim("supplier_name")) == "global supply") & F.col("supplier_name").like("P%"))
    .groupBy("country")
    .agg(
        F.max("rating").alias("max_rating"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("min_rating") > 3.1)
    .drop("min_rating")
    .orderBy(F.col("max_rating").desc())
)
