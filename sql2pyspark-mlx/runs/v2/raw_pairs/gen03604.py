from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.filter(F.col("country").isin(["UK", "TH"])).alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_semi")
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.stock_qty"),
        F.col("products.product_name"),
    )
)
