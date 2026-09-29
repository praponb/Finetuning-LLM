from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%URO%") & (F.col("rating") < 3.5))
    .select(
        "id",
        "rating",
        "supplier_name",
        "country",
        F.round((F.col("rating") * 1.15), 2).alias("rating_adjusted"),
        F.concat("city", F.lit(", "), "supplier_name").alias("city_supplier_name"),
    )
    .orderBy("country", "city_supplier_name", "id")
    .limit(10)
)
