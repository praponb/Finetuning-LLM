from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "cost",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date_2"),
    )
    .orderBy("days_since_shipped_date_2")
)
