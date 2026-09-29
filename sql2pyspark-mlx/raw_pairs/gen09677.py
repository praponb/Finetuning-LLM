from pyspark.sql import functions as F

result = (
    df_shipments
    .select("order_id", "id")
    .intersectAll(
        df_shipments
        .filter(F.col("shipped_date") >= "2025-01-19")
        .select("order_id", "id")
    )
)
