from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("brand") == "Umbrella") & F.col("supplier_id").isNotNull())
    .withColumn("least_stock_qty_price", F.least("stock_qty", "price"))
)

result = (
    cleaned
    .select("id", "brand")
    .subtract(
        df_products
        .filter(F.col("category") != "Sports")
        .select("id", "brand")
    )
)
