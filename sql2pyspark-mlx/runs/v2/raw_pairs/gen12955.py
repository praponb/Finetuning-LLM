from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.cost") < 660)
    .select(
        F.col("p.cost"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.date_trunc("MONTH", F.col("i.last_restock")).alias("last_restock_period"),
    )
    .orderBy(F.col("cost").desc())
)
