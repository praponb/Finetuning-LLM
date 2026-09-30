from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("products.category").isin(["Sports", "Fashion"]))
    .groupBy(
        F.col("suppliers.country"),
        F.when(F.col("suppliers.rating") >= 3.7, "high").when(F.col("suppliers.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum(F.col("products.cost")).alias("total_cost"))
    .filter(F.col("total_cost") > 4300)
    .orderBy("total_cost")
)
