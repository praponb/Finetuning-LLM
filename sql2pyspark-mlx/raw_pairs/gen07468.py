from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-08-27").cast("date"))
    .withColumn("least_price_stock_qty", F.least("price", "stock_qty"))
    .withColumn("category_upper", F.upper("category"))
)

result = (
    valid
    .groupBy("supplier_id")
    .pivot("brand", ["Initech", "Stark", "Acme"])
    .agg(F.max("least_price_stock_qty"))
    .select(
        "supplier_id",
        F.col("Initech").alias("initech"),
        F.col("Stark").alias("stark"),
        F.col("Acme").alias("acme"),
    )
)
