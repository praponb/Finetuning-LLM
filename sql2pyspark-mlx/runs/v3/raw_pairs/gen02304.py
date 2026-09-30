from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", F.round((F.col("cost") * 1.15), 2).alias("cost_scaled"))
    .orderBy("brand", "id")
    .limit(10)
)
