from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([13, 36]))
    .select("id", "stock_qty")
    .intersect(
        df_products
        .filter(F.col("launch_date") >= "2023-05-01")
        .select("id", "stock_qty")
    )
)
