from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.25)
    .select("id", "store_id", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("status").isin(["SHIPPED", "PENDING", "COMPLETED"]))
        .select("id", "store_id", "order_date")
    )
)
