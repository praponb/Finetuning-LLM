from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 380)
    .select("id", "product_name", "launch_date")
    .intersect(
        df_products
        .filter(F.col("brand").isNotNull())
        .select("id", "product_name", "launch_date")
    )
    .orderBy(F.col("product_name").desc())
)
