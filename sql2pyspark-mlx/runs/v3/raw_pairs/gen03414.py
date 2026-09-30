from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.7)
    .select(
        "id",
        "supplier_name",
        "country",
        F.concat_ws("-", "city", "country").alias("city_country_key"),
    )
    .orderBy("city_country_key", "country")
)
