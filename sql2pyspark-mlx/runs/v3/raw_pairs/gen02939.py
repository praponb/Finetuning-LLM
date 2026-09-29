from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(~F.col("p.brand").isin(["Umbrella", "Initech"]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.price"),
        F.col("p.product_name"),
        (F.col("p.cost") * F.col("p.price")).alias("margin"),
    )
)
