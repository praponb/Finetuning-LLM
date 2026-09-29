from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") >= 0.17)
    .select("status")
    .unionByName(
        df_orders
        .filter(~F.col("status").isin(["COMPLETED", "CANCELLED", "SHIPPED"]))
        .select("status")
    )
)
