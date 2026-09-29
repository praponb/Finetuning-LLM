from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.upper(F.trim("supplier_name")) == "GLOBAL SUPPLY") & F.col("supplier_name").like("%Tok%"))
    .select(
        "id",
        "city",
        F.concat("country", F.lit(", "), "supplier_name").alias("country_supplier_name"),
    )
)
