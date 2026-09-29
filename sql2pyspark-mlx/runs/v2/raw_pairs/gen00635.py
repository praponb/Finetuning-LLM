from pyspark.sql import functions as F

avg_discount = df_orders.agg(F.avg("discount")).first()[0]

result = (
    df_orders
    .filter(F.col("discount") > avg_discount)
    .select("id", "discount")
)
