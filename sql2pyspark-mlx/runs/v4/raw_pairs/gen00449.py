from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNull() & (F.col("city") == "Bangkok"))
    .select(
        "id",
        "rating",
        "supplier_name",
        "city",
        F.concat("city", F.lit(", "), "country").alias("city_country"),
    )
    .orderBy("city_country")
)
