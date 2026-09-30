from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Chiang Mai", "London"]))
    .select(
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.country"),
    )
    .orderBy("product_name")
)
