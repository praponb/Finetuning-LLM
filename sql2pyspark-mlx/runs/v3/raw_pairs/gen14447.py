from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2025-02-01", "2025-03-01") & (F.col("category") == "Electronics"))
    .select(
        "id",
        "stock_qty",
        "tags",
        F.month("launch_date").alias("launch_date_month"),
    )
)
