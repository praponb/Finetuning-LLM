from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .groupBy("brand")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .filter(F.col("avg_price") < 51)
    .select("brand", "avg_price")
    .orderBy("brand")
)
