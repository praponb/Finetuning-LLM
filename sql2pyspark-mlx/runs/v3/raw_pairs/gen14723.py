from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.month("delivered_date").alias("delivered_month"),
        F.year("shipped_date").alias("shipped_year"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.countDistinct("cost").alias("unique_statuss"),
        F.max("delivered_date").alias("last_delivered_date"),
    )
    .orderBy("shipped_year", "delivered_month")
    .limit(50)
)
