from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "status",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.substring("status", 1, 2).alias("status_prefix_2"),
    )
)
