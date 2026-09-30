from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") >= "2025-09-29")
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
    )
)
