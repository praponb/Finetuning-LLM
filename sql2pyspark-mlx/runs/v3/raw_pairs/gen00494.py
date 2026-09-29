from pyspark.sql import functions as F

min_discount = df_orders.filter(F.col("customer_id").isin([15, 22, 29])).agg(F.min("discount")).first()[0]

result = (
    df_orders
    .filter(F.col("discount") == min_discount)
    .select("id", "discount")
    .orderBy(F.col("id").desc())
    .limit(1)
)
