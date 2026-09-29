from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "shipped_date", "order_id", F.upper("carrier").alias("carrier_upper"))
)
