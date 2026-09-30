from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") < "2025-10-14") & ((F.col("delivered_date") >= "2023-09-16") | F.col("order_id").isNull()))
    .select(
        "cost",
        "shipped_date",
        F.date_add("delivered_date", 64).alias("delivered_date_plus_64d"),
    )
    .orderBy("shipped_date")
)
