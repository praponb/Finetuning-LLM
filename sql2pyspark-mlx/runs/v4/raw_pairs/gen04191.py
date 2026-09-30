from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-04-29"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

result = (
    base
    .select("id")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isin([3, 27]))
        .select("id")
    )
)
