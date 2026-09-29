from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(26, 79) | F.col("carrier").isin(["UPS", "Kerry", "DHL"]))
    .select(
        "id",
        "order_id",
        "status",
        "carrier",
        F.concat("status", F.lit(" "), "carrier").alias("status_carrier"),
    )
    .orderBy("id", F.col("status").desc())
)
