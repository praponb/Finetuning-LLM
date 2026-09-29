from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        F.date_add("shipped_date", 78).alias("shipped_date_plus_78d"),
    )
    .orderBy(F.col("id").desc(), "status")
)
