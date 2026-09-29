from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("cost").alias("unique_suppliers"),
        F.avg("price").alias("avg_price"),
    )
    .filter(F.col("avg_price") < 900)
    .drop("avg_price")
    .orderBy("unique_suppliers")
)
