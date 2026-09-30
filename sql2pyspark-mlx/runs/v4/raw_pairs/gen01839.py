from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([18, 22, 30]))
    .select("id", "delivered_date", "carrier")
)
