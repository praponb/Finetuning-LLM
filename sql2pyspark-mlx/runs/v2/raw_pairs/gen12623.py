from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
    .filter(F.col("electronics_count") < 10)
    .select("category", "supplier_id", "electronics_count")
    .orderBy(F.col("electronics_count").desc(), "category", "supplier_id")
    .limit(10)
)
