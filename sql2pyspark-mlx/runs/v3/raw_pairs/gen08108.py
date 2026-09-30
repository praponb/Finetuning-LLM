from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([4, 5, 10]))
    .select(
        "id",
        "tags",
        "cost",
        "supplier_id",
        (F.col("stock_qty") + F.col("price")).alias("stock_qty_plus_price"),
        F.when(F.col("brand").isin(["Acme", "Stark"]), "Priority").otherwise("Other").alias("brand_group"),
    )
)
