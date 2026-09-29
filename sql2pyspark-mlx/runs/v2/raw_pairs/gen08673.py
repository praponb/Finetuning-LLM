from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 810)
    .select("customer_id")
    .unionByName(
        df_orders
        .filter(F.col("status").isin(["SHIPPED", "PENDING"]))
        .select("customer_id")
    )
)
