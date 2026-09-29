from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date", "carrier", F.round("cost", 2).alias("cost_rounded"))
)
