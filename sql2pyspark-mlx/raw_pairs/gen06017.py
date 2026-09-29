from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(182, 322))
    .select("id", "launch_date", "stock_qty")
    .subtract(
        df_products
        .filter(F.length("product_name") >= 12)
        .select("id", "launch_date", "stock_qty")
    )
)
