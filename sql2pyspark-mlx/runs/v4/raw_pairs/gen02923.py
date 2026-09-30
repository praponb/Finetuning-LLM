from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNull())
    .select("order_id", "delivered_date")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") != "DHL")
        .select("order_id", "delivered_date")
    )
    .distinct()
)
