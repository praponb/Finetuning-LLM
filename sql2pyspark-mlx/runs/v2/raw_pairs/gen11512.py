from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 3.0) | F.col("country").isin(["JP", "SG"]))
    .select(
        "id",
        "rating",
        "country",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
        F.length("country").alias("country_length"),
    )
    .orderBy(F.col("rating").desc())
)
