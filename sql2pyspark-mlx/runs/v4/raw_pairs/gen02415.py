from pyspark.sql import functions as F

min_discount = df_orders.agg(F.min("discount")).first()[0]

result = (
    df_orders
    .filter(F.col("discount") <= min_discount)
    .select("id", "discount")
    .orderBy(F.col("discount").desc())
)
