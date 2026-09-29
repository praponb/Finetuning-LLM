from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "US"]) & F.col("supplier_name").isNotNull())
    .groupBy("country")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("min_rating") > 3.8)
    .drop("min_rating")
    .orderBy(F.col("total_rating").desc())
)
