from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") <= "2025-08-01") | (F.col("carrier") == "FedEx"))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
)
