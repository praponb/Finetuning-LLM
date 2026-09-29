from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(40, 74))
    .select(
        "id",
        "carrier",
        "cost",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)
