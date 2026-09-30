from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNull())
    .select(
        "id",
        "rating",
        "country",
        F.concat_ws("/", "city", "supplier_name").alias("city_supplier_name_key"),
    )
    .orderBy("city_supplier_name_key", "id")
    .limit(10)
)
