from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
        "order_id",
    )
    .agg(F.countDistinct("cost").alias("unique_carriers"))
    .filter(F.col("unique_carriers") >= 4)
    .select("delivered_month", "order_id", "unique_carriers")
    .orderBy(F.col("unique_carriers").desc(), "delivered_month", "order_id")
    .limit(50)
)
