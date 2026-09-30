from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(~(((F.year("last_restock") >= 2025) & (F.col("last_restock") <= "2025-07-01")) | (F.col("quantity") >= 171)))
)

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    product_id_summary
    .filter(F.col("avg_quantity") < 150)
    .select("product_id", "avg_quantity", "min_last_restock")
    .orderBy(F.col("product_id").desc(), F.col("min_last_restock").desc())
)
