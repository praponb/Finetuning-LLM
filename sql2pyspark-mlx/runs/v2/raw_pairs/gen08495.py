from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & F.col("carrier").isNotNull())
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
    )
)
