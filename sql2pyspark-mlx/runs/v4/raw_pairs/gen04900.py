from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        F.concat_ws("-", "supplier_name", "city").alias("supplier_name_city_key"),
        F.when(F.col("rating") >= 3.9, "High").when(F.col("rating") >= 2.4, "Medium").otherwise("Low").alias("rating_level"),
    )
)
