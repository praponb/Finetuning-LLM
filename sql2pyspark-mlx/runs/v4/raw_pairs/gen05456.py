from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 56)
    .select("status", "delivered_date")
    .subtract(
        df_shipments
        .filter(F.col("status") == "LOST")
        .select("status", "delivered_date")
    )
    .orderBy("status")
)
