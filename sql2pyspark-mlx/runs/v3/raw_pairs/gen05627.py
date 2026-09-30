from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Stark")
    .select("cost")
    .unionByName(df_orders.select(F.col("amount").alias("cost")))
    .distinct()
)
