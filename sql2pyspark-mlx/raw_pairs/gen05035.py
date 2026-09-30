from pyspark.sql import functions as F

src = (
    df_shipments
    .filter((F.col("status") != "LOST") & (F.col("shipped_date") < "2024-07-23"))
)

summary = (
    src
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
    )
)

result = (
    summary
    .filter(F.col("delivered_cost") > 49)
    .select("order_id", "delivered_cost")
    .orderBy(F.col("order_id").desc(), F.col("delivered_cost").desc())
    .limit(5)
)
