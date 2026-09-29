from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") == 351)
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"), "category")
    .agg(F.max("cost").alias("max_cost"))
)
