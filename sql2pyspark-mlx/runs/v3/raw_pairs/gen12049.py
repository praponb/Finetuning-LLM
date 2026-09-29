from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("cost") < 37) & (F.col("carrier") == "FedEx") & F.col("order_id").isin([2, 20]))
)

agg = (
    valid_shipments
    .groupBy(
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
    )
    .agg(F.sum("cost").alias("total_cost"))
)

result = agg
