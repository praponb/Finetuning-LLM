from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city") != "New York")
    .withColumn("city_country_key", F.concat_ws("/", "city", "country"))
)

result = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "bestseller")), "left_anti")
)
