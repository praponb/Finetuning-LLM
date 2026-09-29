from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 31)
    .select("status", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isin([17, 22, 35, 40]))
        .select("status", "carrier")
    )
    .distinct()
)
