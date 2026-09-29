from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Books") & (F.size("tags") >= 1) & F.col("stock_qty").between(159, 176))
    .select(
        "brand",
        "launch_date",
        "product_name",
        F.array_contains("tags", "clearance").alias("is_clearance"),
        F.dayofmonth("launch_date").alias("launch_date_day"),
    )
)
