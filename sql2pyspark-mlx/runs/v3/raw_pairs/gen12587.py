from pyspark.sql import functions as F

result = (
    df_shipments
    .cube(
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
        F.month("shipped_date").alias("shipped_month"),
    )
    .agg(
        F.min("shipped_date").alias("min_shipped_date"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .orderBy("delivered_month")
)
