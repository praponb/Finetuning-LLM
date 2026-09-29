from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select("customer_id", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("customer_id") == 9)
        .select("customer_id", "store_id")
    )
    .distinct()
)
