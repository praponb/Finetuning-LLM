from pyspark.sql import functions as F

src = df_shipments.filter(F.col("carrier") == "Flash")

summary = (
    src
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.min("shipped_date").alias("min_shipped_date"),
    )
)

result = summary.filter(F.col("unique_carriers") < 8)
