from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") == 2024)
    .select(
        "status",
        "shipped_date",
        "carrier",
        (F.datediff(F.current_date(), "shipped_date") / 365.25).alias("years_since_shipped_date"),
    )
)
