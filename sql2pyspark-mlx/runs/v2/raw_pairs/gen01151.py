from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "supplier_name",
        F.substring("country", 1, 4).alias("country_prefix"),
    )
    .orderBy("city")
)
