from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "cost",
        "order_id",
        F.date_format("delivered_date", "yyyy-MM-dd").alias("delivered_date_label"),
        F.round("cost", 1).alias("cost_rounded"),
    )
    .orderBy(F.col("order_id").desc(), "cost_rounded", "id")
    .limit(50)
)
