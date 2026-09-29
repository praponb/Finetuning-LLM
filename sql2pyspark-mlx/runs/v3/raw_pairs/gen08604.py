from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter((F.col("stock_qty") <= 247) & (F.col("brand").isin(["Stark", "Globex", "Umbrella"]) | (F.col("cost") > 390)))
)

summary = (
    valid_products
    .groupBy("brand")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    summary
    .filter(F.col("min_cost") >= 310)
    .orderBy("brand", F.col("min_cost").desc())
    .limit(10)
)
