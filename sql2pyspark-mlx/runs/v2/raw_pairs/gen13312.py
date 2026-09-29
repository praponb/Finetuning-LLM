from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([6, 9, 10]))
    .select("id", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") < "2025-06-01")
        .select("id", "shipped_date")
    )
)
