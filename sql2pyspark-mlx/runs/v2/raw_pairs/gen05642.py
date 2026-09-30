from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") >= 277)
    .withColumn("launch_date_period", F.date_trunc("week", "launch_date"))
    .orderBy(F.col("price").desc(), "category")
)
