from pyspark.sql import functions as F

result = (
    df_orders
    .select("store_id")
    .exceptAll(
        df_orders
        .filter(F.col("order_date") < "2025-03-01")
        .select("store_id")
    )
)
