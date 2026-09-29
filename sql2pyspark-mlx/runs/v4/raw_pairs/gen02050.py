from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "shipping_fee", "discount")
    .orderBy(F.col("id").desc())
    .limit(10)
)
