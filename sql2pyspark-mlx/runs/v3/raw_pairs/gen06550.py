from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2024-02-01")
    .select("id", "order_id", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") <= "2023-09-01")
        .select("id", "order_id", "shipped_date")
    )
)
