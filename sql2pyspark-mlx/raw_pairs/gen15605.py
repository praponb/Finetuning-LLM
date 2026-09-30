from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 3.3) | F.col("supplier_name").like("%Gmb%"))
    .select(
        "id",
        "city",
        F.concat_ws(",", "country", "supplier_name").alias("country_supplier_name_key"),
    )
    .orderBy(F.col("id").desc())
)
