from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") & ((F.col("status") == "IN_TRANSIT") | (F.col("status") == "IN_TRANSIT")))
    .select(
        "id",
        "order_id",
        "cost",
        F.round((F.col("cost") * 100), 2).alias("adjusted_cost"),
    )
)
