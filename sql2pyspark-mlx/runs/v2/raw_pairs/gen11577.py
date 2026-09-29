from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.when(F.col("p.category") == "Books", F.col("p.cost")).otherwise(0)).alias("books_cost"),
        F.round(F.stddev(F.col("p.price")), 2).alias("stddev_price"),
    )
    .orderBy("books_cost", "country")
    .limit(20)
)
