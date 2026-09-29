from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "city",
        "supplier_name",
        "rating",
        F.substring("city", 1, 4).alias("city_prefix"),
    )
)
