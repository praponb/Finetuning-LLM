from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "TH") | F.upper("supplier_name").like("%ION%"))
    .select(
        "supplier_name",
        F.concat("supplier_name", F.lit(", "), "country").alias("supplier_name_country"),
    )
    .orderBy(F.col("supplier_name").desc())
)
