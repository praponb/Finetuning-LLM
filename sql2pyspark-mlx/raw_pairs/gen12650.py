from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.6)
    .select(
        "id",
        "country",
        "rating",
        F.concat_ws(",", "supplier_name", "country").alias("supplier_name_country_key"),
    )
)
