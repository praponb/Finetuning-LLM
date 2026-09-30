from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull() & (F.col("brand") == "Acme"))
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
    .orderBy("electronics_count")
)
