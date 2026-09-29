from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 3)
    .select("status", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("status", "carrier")
    )
    .distinct()
)
