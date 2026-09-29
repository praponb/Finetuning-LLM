from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("ss.units").between(17, 30))
    .groupBy(
        F.when(F.col("s.rating") >= 4.4, "high").when(F.col("s.rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum(F.col("ss.units")).alias("total_units"))
    .filter(F.col("total_units") > 230)
)
