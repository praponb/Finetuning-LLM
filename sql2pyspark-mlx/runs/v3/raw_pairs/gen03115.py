from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("cost")).otherwise(0)).alias("sports_cost"),
        F.max("price").alias("max_price"),
    )
    .orderBy(F.col("max_price").desc(), F.col("sports_cost").desc(), "supplier_id")
    .limit(20)
)
