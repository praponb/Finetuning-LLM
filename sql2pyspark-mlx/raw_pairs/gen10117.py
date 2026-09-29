from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("country") != "US")
    .select(
        "*",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.size(F.col("c.tags")) >= 1), "left_anti")
)
