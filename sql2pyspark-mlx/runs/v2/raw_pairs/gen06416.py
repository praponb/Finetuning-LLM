from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "DHL", "Kerry"]) & F.month("delivered_date").isin([1, 7, 12]) & F.col("cost").between(57, 63))
    .groupBy("order_id", F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(F.countDistinct("cost").alias("unique_statuss"))
    .orderBy("order_id")
)
