from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Chair%"))
    .select("stock_qty", "supplier_id")
    .unionByName(
        df_products
        .filter(F.month("launch_date").isin([5, 6, 7]))
        .select("stock_qty", "supplier_id")
    )
)
