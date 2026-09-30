from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("La%"))
    .select(
        "id",
        "brand",
        "tags",
        "launch_date",
        (F.col("stock_qty") * F.col("cost")).alias("stock_qty_times_cost"),
    )
    .orderBy(F.col("id").desc())
)
