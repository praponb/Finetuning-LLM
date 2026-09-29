from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status", "id")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isin([1, 7, 8, 13]))
        .select("status", "id")
    )
)
