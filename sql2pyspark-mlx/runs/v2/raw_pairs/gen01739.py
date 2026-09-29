from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "DELIVERED", "IN_TRANSIT"]))
    .select("id", "order_id")
    .unionByName(
        df_shipments
        .filter(F.year("shipped_date") >= 2025)
        .select("id", "order_id")
    )
)
