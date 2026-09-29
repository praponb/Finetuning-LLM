from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "London"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.lower("country").alias("country_lower"),
    )
)

result = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").like("%Offi%")), "left_semi")
)
