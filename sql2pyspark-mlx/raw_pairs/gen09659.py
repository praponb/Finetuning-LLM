from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST"]))
    .select(
        "cost",
        (F.datediff(F.current_date(), "shipped_date") / 365).alias("years_since_shipped_date"),
    )
)
