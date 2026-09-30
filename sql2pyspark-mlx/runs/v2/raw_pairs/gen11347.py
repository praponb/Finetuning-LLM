from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Bangkok", "Chiang Mai", "Berlin"]))
    .select(
        "id",
        "supplier_name",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.trim("city").alias("clean_city"),
    )
)
