from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 35) & F.col("brand").isin(["Stark", "Acme", "Globex"]) & F.col("cost").between(300, 330))
    .select(
        "supplier_id",
        "brand",
        (F.col("cost") + F.col("stock_qty")).alias("cost_plus_stock_qty"),
        F.upper("product_name").alias("product_name_upper"),
    )
)
