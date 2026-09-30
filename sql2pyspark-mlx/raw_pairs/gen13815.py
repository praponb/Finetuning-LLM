from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Gl%") & F.col("country").isin(["UK", "US", "DE"]))
    .select(
        "supplier_name",
        "country",
        F.lower("supplier_name").alias("supplier_name_lower"),
    )
    .orderBy(F.col("supplier_name_lower").desc())
)
