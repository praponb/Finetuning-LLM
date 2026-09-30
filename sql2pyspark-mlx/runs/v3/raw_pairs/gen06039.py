from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter((F.col("products.stock_qty") >= 113) & F.col("products.category").isin(["Books", "Fashion", "Home"]))
    .select(
        F.col("suppliers.city"),
        F.col("products.stock_qty"),
        F.col("products.price"),
        F.col("products.product_name"),
        F.month(F.col("products.launch_date")).alias("launch_date_month"),
    )
    .orderBy("price")
)
