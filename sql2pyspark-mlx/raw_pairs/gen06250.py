from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%oky%") & F.col("rating").between(3.1, 3.8))
    .select(
        "id",
        "city",
        "supplier_name",
        "country",
        F.upper("country").alias("country_upper"),
    )
)
