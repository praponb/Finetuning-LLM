from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Gmb%"))
    .select(
        "supplier_name",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)
