from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("customer_id").isNotNull()).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .filter(F.col("s.city") != "New York")
    .select(F.col("s.id"), F.col("s.opened_date"))
)
