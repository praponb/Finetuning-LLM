from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") <= "2023-10-01")
    .select("id", "channel")
    .unionByName(
        df_orders
        .filter(F.col("status").isin(["RETURNED", "PENDING"]))
        .select("id", "channel")
    )
)
