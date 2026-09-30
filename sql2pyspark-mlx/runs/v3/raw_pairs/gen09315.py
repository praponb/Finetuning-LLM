from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "tokyo traders")
    .select(
        "id",
        "supplier_name",
        "rating",
        "country",
        F.floor("rating").alias("rating_whole"),
        F.concat_ws("/", "supplier_name", "country").alias("supplier_name_country_key"),
    )
    .orderBy("rating", "id")
    .limit(20)
)
