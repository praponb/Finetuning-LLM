from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .orderBy("delivered_month")
)
