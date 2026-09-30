from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([1, 5, 26, 33]))
    .select("id", "channel")
    .unionByName(df_orders.filter(F.col("channel") == "store").select("id", "channel"))
    .distinct()
    .orderBy("channel")
)
