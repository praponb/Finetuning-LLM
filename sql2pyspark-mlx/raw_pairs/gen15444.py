from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("shipped_date").alias("latest_shipped_date"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
)
