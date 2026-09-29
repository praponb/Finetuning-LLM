from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
        F.countDistinct("stock_qty").alias("unique_categorys"),
    )
    .filter(F.col("unique_categorys") >= 9)
    .select("brand", "initech_count", "unique_categorys")
)
