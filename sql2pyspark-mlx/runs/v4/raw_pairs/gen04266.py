from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("cost") < 550))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "launch_date",
        "tags",
        "supplier_id",
        F.floor("price").alias("price_whole"),
        (F.col("stock_qty") * F.col("price")).alias("stock_qty_times_price"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

result = (
    valid
    .groupBy("category", "supplier_id")
    .pivot("brand", ["Acme", "Umbrella"])
    .agg(F.max("price_whole").alias("value_sum"))
    .select(
        "category",
        "supplier_id",
        F.col("Acme").alias("acme"),
        F.col("Umbrella").alias("umbrella"),
    )
)
