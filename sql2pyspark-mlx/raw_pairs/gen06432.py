from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("cost") > 310)
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.coalesce("product_name", F.lit("Unknown")).alias("product_name_filled"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

result = (
    valid
    .alias("a")
    .join(df_suppliers.alias("dim"), F.col("a.supplier_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.country").alias("country"), F.col("dim.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.cost")).alias("avg_cost"),
        F.min(F.col("a.stock_qty")).alias("min_stock_qty"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("min_stock_qty").desc())
)
