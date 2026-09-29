from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("sqft") > 9234) & F.col("region").isNotNull())
    .select("id", "city", "region", "sqft", F.upper("city").alias("city_upper"))
)

result = (
    base
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < "2024-02-01"), "left_anti")
)
