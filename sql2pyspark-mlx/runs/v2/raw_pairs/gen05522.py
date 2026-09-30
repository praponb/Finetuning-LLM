from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("product_id") == 1)

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    product_id_summary
    .select("product_id", "revenue_range", "total_units")
    .orderBy(F.col("product_id").desc())
)
