from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select("id", "channel", "discount", "store_id")
    .orderBy(F.col("discount").desc(), "id")
    .limit(1)
)
