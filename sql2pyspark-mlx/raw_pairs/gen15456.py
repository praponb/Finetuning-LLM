from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1330)
    .select("customer_id")
    .intersect(
        df_orders
        .filter(~F.col("status").isin(["PENDING", "CANCELLED"]))
        .select("customer_id")
    )
)
