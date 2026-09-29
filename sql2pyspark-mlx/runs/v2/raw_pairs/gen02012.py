from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 14)
    .select("id", "shipped_date")
    .subtract(
        df_shipments
        .filter(F.month("shipped_date").isin([1, 3, 11]))
        .select("id", "shipped_date")
    )
)
