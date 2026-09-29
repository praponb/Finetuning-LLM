from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.4) & (F.col("city") == "Chiang Mai") & ~F.col("supplier_name").like("%Par%"))
    .select(
        "id",
        "supplier_name",
        "city",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
        F.substring("country", 1, 4).alias("country_prefix"),
    )
    .orderBy(F.col("supplier_name").desc(), "city", "id")
    .limit(20)
)
