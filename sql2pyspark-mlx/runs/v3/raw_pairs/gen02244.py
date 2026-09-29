from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNull() & (F.length("supplier_name") > 14))
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.concat_ws(",", "supplier_name", "country").alias("supplier_name_country_key"),
    )
)
