from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "DHL") & ((F.col("delivered_date") > "2023-12-01") | (F.col("delivered_date") >= "2023-07-26")))
    .groupBy("carrier", "status")
    .agg(F.sum("cost").alias("total_cost"))
    .orderBy("carrier")
)
