from pyspark.sql import functions as F

src = df_products.filter(F.col("stock_qty") < 110)

summary = (
    src
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
        F.max("price").alias("max_price"),
    )
)

result = (
    summary
    .filter(F.col("max_price") > 700)
    .orderBy(F.col("supplier_id").desc(), F.col("max_price").desc())
)
