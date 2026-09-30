from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 46)
    .select(
        "id",
        "cost",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
    )
)
