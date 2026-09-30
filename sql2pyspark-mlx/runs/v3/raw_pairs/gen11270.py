from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy("avg_quantity", F.col("launch_quarter").desc())
)
