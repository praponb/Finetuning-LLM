from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select("order_id", "shipped_date")
    .intersect(
        df_shipments
        .filter(F.col("carrier") == "FedEx")
        .select("order_id", "shipped_date")
    )
)
