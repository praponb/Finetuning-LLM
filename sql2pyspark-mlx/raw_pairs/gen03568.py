from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%EuroGood%"))
    .select(
        "id",
        "supplier_name",
        "city",
        "country",
        F.lower("country").alias("country_lower"),
    )
    .orderBy("country", "id")
    .limit(1)
)
