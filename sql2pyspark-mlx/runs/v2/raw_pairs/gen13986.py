from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(46, 49) & (F.col("status") != "DELIVERED"))
    .select(
        "id",
        "shipped_date",
        "status",
        "carrier",
        F.concat_ws("-", "status", "carrier").alias("status_carrier_key"),
        F.col("cost").cast("int").alias("cost_int"),
    )
    .orderBy("cost_int", "id")
    .limit(10)
)
