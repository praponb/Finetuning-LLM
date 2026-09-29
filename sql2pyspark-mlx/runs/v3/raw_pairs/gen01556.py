from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("store_id") == 6)
    .withColumn("least_amount_discount", F.least("amount", "discount"))
)

agg = (
    base
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.min("order_date").alias("first_order_date"))
)

result = (
    agg
    .select("order_year", "first_order_date")
    .orderBy(F.col("order_year").desc())
)
