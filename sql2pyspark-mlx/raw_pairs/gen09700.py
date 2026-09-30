from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([1, 4, 6]))
    .select(
        "id",
        "carrier",
        "cost",
        F.round((F.datediff("shipped_date", "delivered_date") / 30), 1).alias("months_delivered_date_to_shipped_date"),
    )
)
