from pyspark.sql import functions as F

base = df_shipments.filter(F.col("shipped_date") > "2023-08-01")

agg = (
    base
    .groupBy("status", "carrier")
    .agg(F.countDistinct("cost").alias("unique_statuss"))
)

result = agg.orderBy("unique_statuss")
