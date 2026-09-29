from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .filter((F.col("p.category") != "Fashion") & F.col("p.brand").isin(["Acme", "Stark", "Initech"]))
    .select(
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
)
