from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "web", "store"]))
    .select("id", "status")
    .unionByName(
        df_orders
        .filter(F.month("order_date").isin([4, 11, 12]))
        .select("id", "status")
    )
    .orderBy("id")
)
