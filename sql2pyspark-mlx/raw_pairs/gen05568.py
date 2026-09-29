from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", "created_at", F.inline("items"))

result = (
    flattened
    .select("id", "customer_id", "created_at", "price")
    .orderBy("id")
)
