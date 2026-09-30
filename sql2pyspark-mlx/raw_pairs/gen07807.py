from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
        F.size("attributes").alias("num_attributes"),
    )
)
