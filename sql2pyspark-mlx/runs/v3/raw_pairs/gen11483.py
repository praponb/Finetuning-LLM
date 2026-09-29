from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 7) & F.col("units").between(28, 35))
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("sale_date").alias("first_sale_date"),
    )
    .orderBy(F.col("store_id").desc())
)
