from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("rating") < 2.0) & (F.col("country") != "SG") & F.col("country").isin(["DE", "SG", "UK"])))
    .select(
        "id",
        "supplier_name",
        "rating",
        "city",
        F.concat("country", F.lit(" - "), "supplier_name").alias("country_supplier_name"),
    )
)
