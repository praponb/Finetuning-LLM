from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.2, 2.6))
    .select(
        "supplier_name",
        F.concat_ws("-", "country", "city").alias("country_city_key"),
    )
)
