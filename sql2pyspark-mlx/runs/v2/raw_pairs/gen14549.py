from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "UK", "US"]) & (F.length("supplier_name") >= 14) & (F.col("rating") < 4.0))
    .select(
        "id",
        "country",
        "city",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)
