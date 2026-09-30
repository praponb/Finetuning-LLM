from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "UK"]) & (F.col("city") == "Bangkok"))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.upper("city").alias("city_upper"),
    )
)
