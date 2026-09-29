from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2024-11-01") & F.col("carrier").isin(["UPS", "Kerry"]))
    .agg(F.min("shipped_date").alias("first_shipped_date"))
    .orderBy("first_shipped_date")
)
