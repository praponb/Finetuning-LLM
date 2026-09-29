from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 920)
    .select("category", "stock_qty")
    .unionByName(
        df_products
        .filter(F.size("tags") >= 1)
        .select("category", "stock_qty")
    )
    .distinct()
    .orderBy(F.col("stock_qty").desc())
)
