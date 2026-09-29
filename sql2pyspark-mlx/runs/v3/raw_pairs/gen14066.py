from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("tags_text", F.array_join("tags", "|"))
    .withColumn("least_cost_stock_qty", F.least("cost", "stock_qty"))
)
