from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["DHL", "Flash", "Kerry"]) | F.col("status").isin(["IN_TRANSIT", "DELIVERED", "LOST"]))
    .select("id", "order_id", "carrier", "status")
    .orderBy(F.col("order_id").desc(), "id")
    .limit(10)
)
