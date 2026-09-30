from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([4, 28, 32, 35]) & (F.col("brand") == "Initech") & F.col("price").between(380, 990))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .orderBy("avg_cost")
)
