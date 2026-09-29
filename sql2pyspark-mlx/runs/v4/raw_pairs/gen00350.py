from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.col("ship_to.country").alias("ship_country"),
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
    )
)
