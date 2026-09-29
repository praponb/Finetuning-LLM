from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eur%"))
    .select(
        "id",
        "rating",
        F.round((F.col("rating") * 0.85), 2).alias("adjusted_rating"),
    )
    .orderBy("rating", "id")
    .limit(50)
)
