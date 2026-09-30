from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["DELIVERED", "IN_TRANSIT"]) & ~F.col("status").isin(["RETURNED", "IN_TRANSIT", "DELIVERED"]))
    .select("order_id", "carrier", "shipped_date")
)
