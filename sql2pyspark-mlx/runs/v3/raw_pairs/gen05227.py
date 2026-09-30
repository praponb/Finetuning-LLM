from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") >= 2025)
    .select("launch_date", "stock_qty")
    .unionByName(
        df_products
        .filter(F.col("launch_date") <= "2023-07-29")
        .select("launch_date", "stock_qty")
    )
    .distinct()
)
