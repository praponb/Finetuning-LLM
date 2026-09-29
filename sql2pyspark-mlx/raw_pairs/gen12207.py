from pyspark.sql import functions as F

src = df_shipments.filter((F.col("cost") <= 28) | (F.col("carrier") == "Flash"))

shipments_stats = (
    src
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(F.countDistinct("cost").alias("unique_carriers"))
)

result = (
    shipments_stats
    .select("delivered_month", "unique_carriers")
    .orderBy("unique_carriers")
)
