from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("To%"))
    .select(
        "country",
        "city",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.lower("country").alias("country_lower"),
    )
)
