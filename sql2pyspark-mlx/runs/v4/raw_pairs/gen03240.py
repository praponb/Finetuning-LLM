from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2024-02-21"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.round("stock_qty", 0).alias("stock_qty_rounded"),
        F.coalesce("price", F.lit(0)).alias("price_filled"),
    )
)

result = (
    base
    .groupBy("category")
    .agg(
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty_rounded")).otherwise(0)).alias("sports_stock_qty_rounded"),
    )
    .filter(F.col("corr_price_stock_qty") < 0)
)
