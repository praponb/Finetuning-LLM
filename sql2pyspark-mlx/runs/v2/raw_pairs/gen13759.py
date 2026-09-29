from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.col("supplier_id").isin([1, 16, 28, 32]) | (F.col("launch_date") < "2024-06-03")))
    .select(
        "id",
        "price",
        "supplier_id",
        "stock_qty",
        F.when(F.col("cost") >= 650, "High").when(F.col("cost") >= 120, "Medium").otherwise("Low").alias("cost_level"),
    )
)
