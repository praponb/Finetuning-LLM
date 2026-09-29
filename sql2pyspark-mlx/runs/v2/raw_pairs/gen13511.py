from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Kerry", "Flash"]) | ~F.col("carrier").isin(["Kerry", "Flash"]))
    .select(
        "id",
        "cost",
        "shipped_date",
        "status",
        F.round("cost", 1).alias("cost_rounded"),
    )
    .orderBy("status")
)
