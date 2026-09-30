from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("T%") & (F.length("supplier_name") >= 6))
    .select(
        "country",
        F.when(F.col("country").isin(["JP", "UK"]), "Tier 1").otherwise("Other").alias("country_group"),
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
    )
)
