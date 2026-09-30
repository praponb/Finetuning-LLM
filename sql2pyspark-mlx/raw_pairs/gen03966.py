from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & (F.col("status").isin(["RETURNED", "IN_TRANSIT"]) | F.col("carrier").isin(["UPS", "Flash", "FedEx"])))
    .agg(F.min("shipped_date").alias("first_shipped_date"))
)
