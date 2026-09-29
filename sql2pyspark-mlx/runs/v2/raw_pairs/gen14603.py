from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 4.0) & F.col("country").isin(["US", "TH"]) & (F.col("city") == "Bangkok"))
    .select(
        "supplier_name",
        F.concat("supplier_name", F.lit(", "), "city").alias("supplier_name_city"),
    )
)
