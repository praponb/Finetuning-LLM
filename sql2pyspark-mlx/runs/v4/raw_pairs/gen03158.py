from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("clean_country", F.trim("country"))
    .withColumn("city_clean", F.coalesce("city", F.lit("OTHER")))
)

filtered_suppliers = (
    filtered
    .filter(F.col("city").isin(["New York", "Chiang Mai"]))
    .select(
        "*",
        F.trim("city").alias("clean_city"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean_2"),
    )
)

kept = (
    filtered_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.upper(F.col("c.product_name")).like("%OIS%")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "clean_country",
        "city_clean",
        "clean_city",
        "city_clean_2",
        F.when(F.col("city").rlike("^(Tokyo|Bangkok)$"), "EXTENDED").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "PRIORITY").otherwise("GROUP_A").alias("city_zone"),
    )
)
