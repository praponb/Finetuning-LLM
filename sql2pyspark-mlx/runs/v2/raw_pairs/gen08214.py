from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("cost") >= 350) & (F.col("launch_date") > "2024-08-16")) | F.col("category").isin(["Books", "Grocery"]))
    .select(
        "category",
        "tags",
        "stock_qty",
        F.date_format("launch_date", "yyyy-MM").alias("launch_date_label"),
    )
)
