from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 33)
    .groupBy("category")
    .agg(F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"))
    .filter(F.col("stark_count") < 40)
)
