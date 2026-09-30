from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") < "2024-07-02") & (F.col("cost") > 40))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("carrier_title", F.initcap(F.trim("carrier")))
    .orderBy(F.col("status_prefix").desc(), "shipped_date", "id")
    .limit(20)
)
