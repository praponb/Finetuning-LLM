from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale") & (F.col("category") == "Books"))
    .groupBy(
        F.quarter("launch_date").alias("launch_quarter"),
        "category",
        F.when(F.col("brand").isin(["Acme", "Globex"]), "priority").otherwise("other").alias("brand_group"),
    )
    .agg(F.max("stock_qty").alias("max_stock_qty"))
    .orderBy("category")
)
