from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("s.country") != "JP")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.when(F.col("s.rating") >= 4.6, "High").when(F.col("s.rating") >= 2.3, "Medium").otherwise("Low").alias("rating_level"),
    )
    .orderBy(F.col("cost").desc(), F.col("id").desc())
)
