from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "DE"]) & (F.col("supplier_name").isNotNull() | (F.col("rating") <= 3.9)))
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
    )
)
