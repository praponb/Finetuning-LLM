from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(33, 90))
    .select("shipped_date", "delivered_date")
    .unionByName(
        df_shipments
        .filter(F.col("order_id") == 34)
        .select("shipped_date", "delivered_date")
    )
)
