from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "store_id", "discount")
    .orderBy("discount", F.col("store_id").desc(), "id")
    .limit(20)
)
