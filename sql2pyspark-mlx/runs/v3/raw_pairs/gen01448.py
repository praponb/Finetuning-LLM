from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "TH") | F.col("supplier_name").isNotNull())
    .select(
        "city",
        "supplier_name",
        "rating",
        F.concat("supplier_name", F.lit(", "), "city").alias("supplier_name_city"),
        F.substring("city", 1, 2).alias("city_prefix"),
    )
)
