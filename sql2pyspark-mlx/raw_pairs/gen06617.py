from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("s.supplier_name"),
        F.col("p.category"),
        F.col("p.product_name"),
        F.concat_ws(",", F.col("s.country"), F.col("s.city")).alias("country_city_key"),
    )
)
