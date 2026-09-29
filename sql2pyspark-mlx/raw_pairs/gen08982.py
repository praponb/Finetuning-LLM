from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.countDistinct("cost").alias("unique_carriers"))
    .filter(F.col("unique_carriers") >= 7)
    .orderBy(F.col("carrier").desc())
    .limit(5)
)
