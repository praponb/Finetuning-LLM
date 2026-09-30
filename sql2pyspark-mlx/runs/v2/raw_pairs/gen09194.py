from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([19, 22, 26, 33]))
    .orderBy(F.col("status").desc(), "id")
    .limit(20)
)
