from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([8, 14, 16, 37]))
    .select(
        "id",
        "status",
        "carrier",
        F.when(F.col("status").isin(["RETURNED", "IN_TRANSIT"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy(F.col("status").desc())
)
