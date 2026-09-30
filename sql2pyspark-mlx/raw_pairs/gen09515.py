from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 2.4) | ~F.col("country").isin(["US", "JP"]))
    .select(
        "id",
        "rating",
        "city",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.lower("country").alias("country_lower"),
    )
    .orderBy(F.col("rating").desc(), "country_lower")
)
