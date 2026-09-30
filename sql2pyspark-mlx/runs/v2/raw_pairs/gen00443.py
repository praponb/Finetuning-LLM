from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.supplier_name").like("S%"))
    .groupBy(
        F.when(F.col("s.rating") >= 4.6, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(
        F.sum(F.when(F.col("p.category") == "Fashion", F.col("p.stock_qty")).otherwise(0)).alias("fashion_stock_qty"),
        F.sort_array(F.collect_set(F.col("p.category"))).alias("category_list"),
    )
    .orderBy("country", "fashion_stock_qty")
)
