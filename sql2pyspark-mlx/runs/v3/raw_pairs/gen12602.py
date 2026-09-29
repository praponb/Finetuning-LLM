from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
    )
    .orderBy("carrier")
)
