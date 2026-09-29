from pyspark.sql import functions as F

base = df_products.filter(F.col("product_name").isNotNull())

agg = (
    base
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost")).otherwise(0)).alias("globex_cost"),
        F.max("price").alias("max_price"),
    )
)

result = (
    agg
    .filter(F.col("max_price") > 1030)
    .select("brand", "globex_cost", "max_price")
    .orderBy("brand", F.col("globex_cost").desc())
)
