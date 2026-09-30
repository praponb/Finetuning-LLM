from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([22, 27, 33, 35]))
    .select("id", "quantity", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id") == 31)
        .select("id", "quantity", "product_id")
    )
    .distinct()
    .orderBy("id")
)
