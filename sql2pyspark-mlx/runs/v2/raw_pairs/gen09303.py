from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Siam%") & (F.col("rating") <= 2.1))
    .select(
        "id",
        "rating",
        F.when(F.col("rating") >= 3.5, "High").when(F.col("rating") >= 2.5, "Medium").otherwise("Low").alias("rating_level"),
    )
    .orderBy(F.col("rating").desc())
)
