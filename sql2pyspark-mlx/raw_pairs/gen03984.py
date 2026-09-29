from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("store_id").isNotNull())
    .orderBy(F.col("amount").desc())
)
