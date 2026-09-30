from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(((F.year("shipped_date") == 2024) & F.col("order_id").isNotNull()) | (F.col("delivered_date") < "2023-08-02")))
    .select(
        "id",
        "order_id",
        F.substring("carrier", 1, 3).alias("carrier_prefix"),
        F.round((F.col("cost") * 1.15), 2).alias("cost_adjusted"),
    )
)
