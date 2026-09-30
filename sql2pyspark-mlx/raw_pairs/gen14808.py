from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .filter(F.col("b.launch_date").between("2024-06-01", "2025-04-01"))
    .select(
        "a.id",
        "a.product_id",
        "a.sale_date",
        F.col("b.cost"),
        F.col("b.product_name"),
    )
)
