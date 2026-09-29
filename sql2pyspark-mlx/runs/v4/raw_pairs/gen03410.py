from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("products.brand").isNull())
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.3, "high").when(F.col("suppliers.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum(F.col("products.cost")).alias("total_cost"))
)
