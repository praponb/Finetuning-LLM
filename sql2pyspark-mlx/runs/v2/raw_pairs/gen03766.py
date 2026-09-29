from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 1160)
    .select("launch_date", "stock_qty")
    .unionByName(
        df_products
        .filter(F.size("tags") > 1)
        .select("launch_date", "stock_qty")
    )
)
