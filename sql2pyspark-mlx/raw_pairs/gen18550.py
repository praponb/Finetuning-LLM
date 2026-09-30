from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city") != "Chiang Mai"))
    .withColumn("clean_country", F.trim("country"))
    .withColumn("city_filled", F.coalesce("city", F.lit("Unknown")))
)

result = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "premium")), "left_anti")
)
