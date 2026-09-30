from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select("store_id", "status")
    .unionByName(
        df_orders
        .filter(F.col("order_date") > "2025-07-03")
        .select("store_id", "status")
    )
    .distinct()
)
