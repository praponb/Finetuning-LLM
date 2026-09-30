from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 89)
    .select("id", "status", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("id", "status", "shipped_date")
    )
)
