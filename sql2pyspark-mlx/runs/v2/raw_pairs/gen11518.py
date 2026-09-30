from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-03-12", "2024-07-01") & (F.col("supplier_id") == 38))
    .select(
        "id",
        "cost",
        "product_name",
        (F.col("stock_qty") + F.col("cost")).alias("stock_qty_plus_cost"),
    )
    .orderBy(F.col("stock_qty_plus_cost").desc(), F.col("id").desc())
)
