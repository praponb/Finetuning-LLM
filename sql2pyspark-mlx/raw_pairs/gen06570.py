from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((~F.col("country").isin(["DE", "UK", "US"]) & F.col("city").isNull()) | F.col("supplier_name").like("%Whole%"))
    .select(
        "country",
        "supplier_name",
        "city",
        F.concat_ws("-", "supplier_name", "country").alias("supplier_name_country_key"),
    )
    .orderBy(F.col("supplier_name").desc())
)
