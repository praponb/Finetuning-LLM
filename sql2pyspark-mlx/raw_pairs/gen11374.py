from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-07-06")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.lower("product_name").alias("product_name_lower"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.supplier_name"), F.col("s.country"), F.col("s.rating"))
)
