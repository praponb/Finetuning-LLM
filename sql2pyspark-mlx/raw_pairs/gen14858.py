from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.col("ship_to.country").alias("ship_country"),
        F.size("items").alias("num_items"),
    )
)
