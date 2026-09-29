from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("p.launch_date")).isin([1, 4, 12]))
    .select(
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("s.country"),
        F.concat_ws("/", F.col("s.city"), F.col("s.country")).alias("city_country_key"),
    )
    .orderBy(F.col("stock_qty").desc())
)
