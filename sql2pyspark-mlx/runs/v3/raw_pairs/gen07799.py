from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 24)
    .select("order_id", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") >= "2023-05-01")
        .select("order_id", "carrier")
    )
    .orderBy("order_id")
)
