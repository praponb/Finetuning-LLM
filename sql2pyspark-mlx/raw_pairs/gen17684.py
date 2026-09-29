from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2023-06-01")
    .select(
        "id",
        "delivered_date",
        "shipped_date",
        "status",
        F.round((F.datediff("delivered_date", "shipped_date") / 30), 1).alias("months_shipped_date_to_delivered_date"),
    )
    .orderBy(F.col("months_shipped_date_to_delivered_date").desc(), "status", "id")
    .limit(10)
)
