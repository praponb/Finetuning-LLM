from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(((F.col("price") <= 590) & F.col("category").isin(["Fashion", "Books"])) | (F.col("supplier_id") == 31)))
    .select(
        "id",
        "category",
        "stock_qty",
        "supplier_id",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.sort_array("tags").alias("sorted_tags"),
    )
)
