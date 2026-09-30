from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1420)
    .select("order_date", "channel")
    .unionByName(
        df_orders
        .filter(~F.col("status").isin(["PENDING", "COMPLETED", "RETURNED"]))
        .select("order_date", "channel")
    )
    .distinct()
    .orderBy(F.col("channel").desc())
)
