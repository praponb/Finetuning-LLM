from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") > 159)
    .select("launch_date")
    .intersect(
        df_products
        .filter(F.col("category").isin(["Books", "Electronics"]))
        .select("launch_date")
    )
)
