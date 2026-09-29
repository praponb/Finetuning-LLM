from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "store"]))
    .select("id", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("amount").between(1120, 1560))
        .select("id", "customer_id")
    )
    .distinct()
)
