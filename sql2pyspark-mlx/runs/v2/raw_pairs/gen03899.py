from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([28, 36]) & (F.col("stock_qty") > 158))
    .groupBy("brand")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)
