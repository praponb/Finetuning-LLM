from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Chiang Mai", "London"]))
    .select(
        "id",
        "supplier_name",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
