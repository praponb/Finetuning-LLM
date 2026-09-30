from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("country") != "DE") & (F.col("rating") <= 2.4) & F.col("supplier_name").like("%Lio%")))
    .select(
        "country",
        "rating",
        F.lower("country").alias("country_lower"),
        F.length("country").alias("country_length"),
    )
)
