from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("cost") >= 670) & (F.col("cost") >= 300)) | (F.col("price") > 490))
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)
