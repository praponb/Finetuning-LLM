from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "SG", "DE"]) & (F.col("city") == "New York") & F.col("country").isin(["UK", "DE"]))
    .select(
        "id",
        "supplier_name",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
    )
)
