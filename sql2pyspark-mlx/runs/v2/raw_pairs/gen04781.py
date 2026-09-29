from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2023-12-17") & ((F.col("delivered_date") > "2023-09-01") | (F.col("cost") >= 92)))
    .agg(
        F.min("cost").alias("min_cost"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
    )
    .orderBy(F.col("dhl_cost").desc())
)
