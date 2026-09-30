from pyspark.sql import functions as F

src = df_shipments.filter(~F.col("carrier").isin(["Flash", "FedEx"]))

agg = (
    src
    .groupBy("carrier")
    .agg(F.min("shipped_date").alias("first_shipped_date"))
)

result = (
    agg
    .orderBy("first_shipped_date", "carrier")
    .limit(5)
)
