from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "DHL", "FedEx"]))
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)
